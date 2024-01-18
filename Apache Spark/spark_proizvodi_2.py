from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import psycopg2

# PostgreSQL connection details
db_params = {
    "host": "prakticnirad_postgres_2_1",
    "port": 5432,
    "user": "postgres",
    "password": "Rea123Teo",
    "database": "warehouse"
}

# Connect to PostgreSQL
connection = psycopg2.connect(**db_params)
cursor = connection.cursor()

# Define the new table name
new_table_name = "Transakcije_proizvodi"

drop_table_sql_transakcije = f"DROP TABLE IF EXISTS {new_table_name};"
cursor.execute(drop_table_sql_transakcije)
connection.commit()

create_table_sql = f"""
    CREATE TABLE {new_table_name} (
        id_transakcije INTEGER PRIMARY KEY,
        barkod_id INTEGER,
        id_trgovine INTEGER,
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

# Step 1: Create SparkSession
spark = SparkSession.builder \
    .appName("Proizvodi") \
    .getOrCreate()

# Step 2: Configure PostgreSQL connection details
database_url = "jdbc:postgresql://prakticnirad_postgres_1:5432/transakcije"
database_properties = {
    "user": "postgres",
    "password": "Rea123Teo",
    "driver": "org.postgresql.Driver"
}

# Step 3: Load data from PostgreSQL
table_name = "Proizvodi"
df = spark.read.jdbc(url=database_url, table=table_name, properties=database_properties)

# Step 4: Transform Data - Deduplicate and Assign New IDs
window_spec = Window.partitionBy("naziv_proizvoda", "cijena", "proizvodjac", "kategorija")
df_transformed = df.withColumn("new_barkod_id", F.first("barkod_id").over(window_spec))

df_transformed = df_transformed.select(
    "barkod_id", "naziv_proizvoda", "cijena", "proizvodjac", "kategorija", "new_barkod_id"
)

# Step 5: Write the transformed data to the new table in warehouse database
warehouse_url = "jdbc:postgresql://prakticnirad_postgres_2_1:5432/warehouse"

# Step 6: Update Transakcije table with new barkod_id
transakcije_table_name = "Transakcije"
df_transakcije = spark.read.jdbc(url=database_url, table=transakcije_table_name, properties=database_properties)


df_updated_transakcije = df_transakcije.join(df_transformed.select("barkod_id", "new_barkod_id"), on="barkod_id", how="left_outer")

# Check if "barkod_id" is different from "new_barkod_id" and update accordingly
df_updated_transakcije = df_updated_transakcije.withColumn(
    "barkod_id",
    F.when(F.col("barkod_id") != F.col("new_barkod_id"), F.col("new_barkod_id")).otherwise(F.col("barkod_id"))
)

df_updated_transakcije = df_updated_transakcije.drop("new_barkod_id")

# Write the updated DataFrame to the warehouse table
df_updated_transakcije.write.jdbc(url=warehouse_url, table="Transakcije_proizvodi", mode="append", properties=database_properties)

df_transformed = df_transformed.dropDuplicates(["naziv_proizvoda", "cijena", "proizvodjac", "kategorija"])

df_transformed = df_transformed.select(
    "barkod_id", "naziv_proizvoda", "cijena", "proizvodjac", "kategorija"
)

df_transformed.write.jdbc(url=warehouse_url, table=table_name, mode="append", properties=database_properties)

# Step 7: Stop SparkSession
spark.stop()
