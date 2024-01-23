import psycopg2
from psycopg2 import sql
from faker import Faker
import random
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, rand

faker = Faker()
start_date = datetime.date(2020, 1, 1)
end_date = datetime.date(2024, 12, 31)

# Step 1: Create SparkSession
spark = SparkSession.builder \
    .appName("ProizvodiKopiranje") \
    .getOrCreate()

# Step 2: Configure PostgreSQL connection details
database_url = "jdbc:postgresql://postgres_2:5432/warehouse"
database_properties = {
    "user": "postgres",
    "password": "Rea123Teo",
    "driver": "org.postgresql.Driver"
}

# Step 3: Load data from PostgreSQL
table_name = "Proizvodi"
df = spark.read.jdbc(url=database_url, table=table_name, properties=database_properties)

existing_barkod_ids_set = set(df.select("barkod_id").distinct().rdd.flatMap(lambda x: x).collect())
new_barkod_ids = []

rows_to_update = df.orderBy(rand()).limit(30)

for row in rows_to_update.collect():
    unique_barkod_id = None
    while unique_barkod_id is None or unique_barkod_id in existing_barkod_ids_set:
        unique_barkod_id = faker.ean(length=8)
    new_barkod_ids.append((row["barkod_id"], unique_barkod_id))
    existing_barkod_ids_set.add(unique_barkod_id)

# Add the new_barkod_ids list to the DataFrame
df_updated = df.withColumn(
    "barkod_id",
    when(col("barkod_id").isin([row[0] for row in new_barkod_ids]), col("barkod_id"))
     .otherwise(col("barkod_id"))
)

# Replace the barkod_id values with the new ones for identified rows
for row in new_barkod_ids:
    df_updated = df_updated.withColumn("barkod_id", when(col("barkod_id") == row[0], row[1]).otherwise(col("barkod_id")))

df_updated = df_updated.withColumn("barkod_id", col("barkod_id").cast("int"))

# PostgreSQL connection details
db_params = {
    "host": "postgres_3",
    "port": 5432,
    "user": "postgres",
    "password": "Rea123Teo",
    "database": "skladiste"
}

# Connect to PostgreSQL
connection = psycopg2.connect(**db_params)
cursor = connection.cursor()

# Define the new table name
new_table_name = "Proizvodi"

drop_table_sql = f"DROP TABLE IF EXISTS {new_table_name};"
cursor.execute(drop_table_sql)
connection.commit()

# Define the SQL statement to create the new table
create_table_sql = f"""
    CREATE TABLE {new_table_name} (
        barkod_id INTEGER PRIMARY KEY,
        naziv_proizvoda VARCHAR(255) NOT NULL,
        cijena NUMERIC(10, 2) NOT NULL,
        proizvodjac VARCHAR(255),
        kategorija VARCHAR(255),
        CONSTRAINT unique_barkod_id UNIQUE (barkod_id)
    )
"""

# Execute the SQL statement to create the new table
cursor.execute(create_table_sql)

# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()


# Step 5: Write the transformed data to the new table in warehouse database
skladiste_url = "jdbc:postgresql://postgres_3:5432/skladiste"
df_updated.write.jdbc(url=skladiste_url, table=table_name, mode="append", properties=database_properties)

# Step 7: Stop SparkSession
spark.stop()

# Database connection parameters
db_params = {
    "host": "postgres_2",
    "port": 5432,
    "user": "postgres",
    "password": "Rea123Teo",
    "database": "warehouse"
}

# Establish a connection to the database
connection = psycopg2.connect(**db_params)

# Create a cursor object to execute SQL queries
cursor = connection.cursor()

# SQL query to fetch all values from the "Lokacija" column in the "Trgovine" table
query = "SELECT DISTINCT lokacija FROM Trgovine;"

# Execute the query
cursor.execute(query)

# Fetch all values
lokacija_values = [value[0] for value in cursor.fetchall()]

# Close the cursor and connection
cursor.close()
connection.close()

db_params = {
    "host": "postgres_3",
    "port": 5432,
    "user": "postgres",
    "password": "Rea123Teo",
    "database": "skladiste"
}

# Connect to PostgreSQL
connection = psycopg2.connect(**db_params)
cursor = connection.cursor()

# Define the new table name
new_table_name = "Skladista"

drop_table_sql = f"DROP TABLE IF EXISTS {new_table_name};"
cursor.execute(drop_table_sql)
connection.commit()

# Define the SQL statement to create the new table
create_table_sql = f"""
    CREATE TABLE {new_table_name} (
        id_skladista SERIAL PRIMARY KEY,
        lokacija VARCHAR(255) NOT NULL,
        CONSTRAINT unique_id_skladista UNIQUE (id_skladista)
    )
"""

# Execute the SQL statement to create the new table
cursor.execute(create_table_sql)

# Commit the changes
connection.commit()

for lokacija in lokacija_values:
    insert_sql = f"INSERT INTO {new_table_name} (lokacija) VALUES ('{lokacija}');"
    cursor.execute(insert_sql)

connection.commit()

# SQL statement to create the Zalihe table
create_zalihe_table_sql = """
    CREATE TABLE Zalihe (
        id_zalihe SERIAL PRIMARY KEY,
        barkod_id INTEGER REFERENCES Proizvodi(barkod_id),
        id_skladista INTEGER REFERENCES Skladista(id_skladista),
        datum_proizvodnje VARCHAR(50),
        datum_isteka VARCHAR(50),
        dostupna_kolicina INTEGER
    );
"""

# Execute the SQL statement
cursor.execute(create_zalihe_table_sql)

# Commit the changes
connection.commit()


# Fetch all distinct barkod_ids from the Proizvodi table
cursor.execute("SELECT DISTINCT barkod_id FROM Proizvodi;")
barkod_ids = [row[0] for row in cursor.fetchall()]

# Fetch all Skladista data
cursor.execute("SELECT id_skladista FROM Skladista;")
skladista_ids = [row[0] for row in cursor.fetchall()]

def generate_random_zalihe_data(barkod_ids, id_skladista, num_proizvodi, used_barkod_ids, date_formats):
    zalihe_data = []

    for _ in range(num_proizvodi):
        available_barkod_ids = list(set(barkod_ids) - set(used_barkod_ids.get(id_skladista, [])))
        
        if not available_barkod_ids:
            break

        chosen_barkod_id = random.choice(available_barkod_ids)

        # Generate production date
        datum_proizvodnje = faker.date_between(start_date=start_date, end_date=end_date)

        # Generate expiration date based on production date
        datum_isteka = faker.date_between(start_date=datum_proizvodnje, end_date=end_date)

        # Ensure that datum_isteka is at least one day after datum_proizvodnje
        while datum_proizvodnje < datum_isteka:
            datum_isteka = faker.date_between(start_date=datum_proizvodnje, end_date=end_date)

        dostupna_kolicina = random.randint(50, 200)

        date_format = random.choice(date_formats)

        zalihe_data.append({
            "barkod_id": chosen_barkod_id,
            "id_skladista": id_skladista,
            "datum_proizvodnje": datum_proizvodnje.strftime(date_format),
            "datum_isteka": datum_isteka.strftime(date_format),
            "dostupna_kolicina": dostupna_kolicina
        })

        used_barkod_ids.setdefault(id_skladista, []).append(chosen_barkod_id)

    return zalihe_data

used_barkod_ids = {}  # Dictionary to keep track of used barkod_ids for each Skladiste
date_formats = ["%Y-%m-%d", "%d/%m/%Y"]

for skladista_id in skladista_ids:
    num_proizvodi = random.randint(10, 30)
    zalihe_data = generate_random_zalihe_data(barkod_ids, skladista_id, num_proizvodi, used_barkod_ids, date_formats)

    # SQL statement to insert values into the Zalihe table
    insert_zalihe_sql = """
        INSERT INTO Zalihe (barkod_id, id_skladista, datum_proizvodnje, datum_isteka, dostupna_kolicina)
        VALUES (%s, %s, %s, %s, %s);
    """

    # Insert Zalihe data into the Zalihe table
    for data in zalihe_data:
        cursor.execute(
            insert_zalihe_sql,
            (
                data["barkod_id"],
                data["id_skladista"],
                data["datum_proizvodnje"],
                data["datum_isteka"],
                data["dostupna_kolicina"]
            )
        )

# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()
