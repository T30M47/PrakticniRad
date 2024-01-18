from pyspark.sql import SparkSession
import psycopg2
from pyspark.sql import functions as F


# PostgreSQL connection details
db_params = {
    "host": "prakticnirad_postgres_2_1",
    "port": 5432,
    "user": "postgres",
    "password": "Rea123Teo",
    "database": "warehouse"
}

warehouse_url = "jdbc:postgresql://prakticnirad_postgres_2_1:5432/warehouse"

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

# Step 4: Transform Data - Format Price and Deduplicate
columns_to_check_duplicates = ["naziv_proizvoda", "cijena", "proizvodjac", "kategorija"]
df_transformed = df.dropDuplicates(subset=columns_to_check_duplicates)

# Step 5: Write the transformed data to the new table in warehouse database
df_transformed.write.jdbc(url=warehouse_url, table=table_name, mode="append", properties=database_properties)

# Step 6: Execute ETL Job
spark.stop()
