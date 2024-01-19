from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import psycopg2

# PostgreSQL connection details
db_params = {
    "host": "postgres_2",
    "port": 5432,
    "user": "postgres",
    "password": "Rea123Teo",
    "database": "warehouse"
}

# Connect to PostgreSQL
connection = psycopg2.connect(**db_params)
cursor = connection.cursor()

# Define the new table name
new_table_name = "Trgovine"

drop_table_sql = f"DROP TABLE IF EXISTS {new_table_name};"
cursor.execute(drop_table_sql)
connection.commit()

# Define the SQL statement to create the new table
create_table_sql = f"""
    CREATE TABLE {new_table_name} (
        id_trgovine INTEGER PRIMARY KEY,
        naziv_trgovine VARCHAR(255) NOT NULL,
        lokacija VARCHAR(255) NOT NULL,
        CONSTRAINT unique_id_trgovine UNIQUE (id_trgovine)
    )
"""

# Execute the SQL statement to create the new table
cursor.execute(create_table_sql)

# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()

# Step 1: Create SparkSession
spark = SparkSession.builder \
    .appName("Trgovine") \
    .getOrCreate()

# Step 2: Configure PostgreSQL connection details
database_url = "jdbc:postgresql://postgres_1:5432/transakcije"
database_properties = {
    "user": "postgres",
    "password": "Rea123Teo",
    "driver": "org.postgresql.Driver"
}

# Step 3: Load data from PostgreSQL
table_name = "Trgovine"
df = spark.read.jdbc(url=database_url, table=table_name, properties=database_properties)

# Step 4: Transform Data - Deduplicate and Assign New IDs
window_spec = Window.partitionBy("naziv_trgovine", "lokacija")
df_transformed = df.withColumn("new_id_trgovine", F.first("id_trgovine").over(window_spec))

df_transformed = df_transformed.select(
    "id_trgovine", "naziv_trgovine", "lokacija", "new_id_trgovine"
)

# Step 5: Write the transformed data to the new table in warehouse database
warehouse_url = "jdbc:postgresql://postgres_2:5432/warehouse"

# Step 6: Update Transakcije table with new barkod_id
transakcije_table_name = "Transakcije_proizvodi"
df_transakcije = spark.read.jdbc(url=warehouse_url, table=transakcije_table_name, properties=database_properties)

df_updated_transakcije = df_transakcije.join(df_transformed.select("id_trgovine", "new_id_trgovine"), on="id_trgovine", how="left_outer")

# Check if "barkod_id" is different from "new_barkod_id" and update accordingly
df_updated_transakcije = df_updated_transakcije.withColumn(
    "id_trgovine",
    F.when(F.col("id_trgovine") != F.col("new_id_trgovine"), F.col("new_id_trgovine")).otherwise(F.col("id_trgovine"))
)

df_updated_transakcije = df_updated_transakcije.drop("new_id_trgovine")

df_trgovine = df_transformed.dropDuplicates(["naziv_trgovine", "lokacija"])

df_trgovine = df_transformed.select(
    "id_trgovine", "naziv_trgovine", "lokacija"
)

df_trgovine.write.jdbc(url=warehouse_url, table=table_name, mode="append", properties=database_properties)

# Connect to PostgreSQL
connection = psycopg2.connect(**db_params)
cursor = connection.cursor()

# Define the new table name
new_table_name = "Transakcije_trgovine"

drop_table_sql_transakcije = f"DROP TABLE IF EXISTS {new_table_name};"
cursor.execute(drop_table_sql_transakcije)
connection.commit()

create_table_sql = f"""
    CREATE TABLE {new_table_name} (
        id_transakcije INTEGER PRIMARY KEY,
        barkod_id INTEGER REFERENCES Proizvodi(barkod_id),
        id_trgovine INTEGER REFERENCES Trgovine(id_trgovine),
        kolicina INTEGER NOT NULL,
        ukupna_cijena DOUBLE PRECISION NOT NULL,
        datum_transakcije DATE NOT NULL,
        popust VARCHAR(5)
    )
"""

# Execute the SQL statement to create the new table
cursor.execute(create_table_sql)

# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()


df_updated_transakcije.write.jdbc(url=warehouse_url, table=new_table_name, mode="append", properties=database_properties)

# Step 7: Stop SparkSession
spark.stop()
