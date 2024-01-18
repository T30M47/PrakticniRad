from pyspark.sql import SparkSession
from pyspark.sql.functions import format_number, regexp_replace, year, month, dayofweek, dayofmonth
import psycopg2
from pyspark.sql.window import Window
from pyspark.sql import functions as F


# PostgreSQL connection details
db_params = {
    "host": "prakticnirad_postgres_2_1",
    "port": 5432,
    "user": "postgres",
    "password": "Rea123Teo",
    "database": "warehouse"
}

db_params_tran = {
    "host": "prakticnirad_postgres_1",
    "port": 5432,
    "user": "postgres",
    "password": "Rea123Teo",
    "database": "transakcije"
}

warehouse_url = "jdbc:postgresql://prakticnirad_postgres_2_1:5432/warehouse"
database_url = "jdbc:postgresql://prakticnirad_postgres_1:5432/transakcije"

connection = psycopg2.connect(**db_params_tran)
cursor = connection.cursor()

update_transakcije_proizvodi = """
    UPDATE Transakcije
    SET barkod_id = new_proizvodi.new_barkod_id
    FROM (
        SELECT common_column, MAX(barkod_id) AS new_barkod_id
        FROM Proizvodi
        GROUP BY common_column
    ) AS new_proizvodi
    WHERE Transakcije.common_column = new_proizvodi.common_column;
"""

update_transakcije_trgovine = """
    UPDATE Transakcije
    SET id_trgovine = new_trgovine.new_id_trgovine
    FROM (
        SELECT common_column, MAX(id_trgovine) AS new_id_trgovine
        FROM Trgovine
        GROUP BY common_column
    ) AS new_trgovine
    WHERE Transakcije.common_column = new_trgovine.common_column;
"""

# Execute the UPDATE statements
cursor.execute(update_transakcije_proizvodi)
cursor.execute(update_transakcije_trgovine)

# Commit the changes
connection.commit()


# Close the cursor and connection
cursor.close()
connection.close()


# Step 1: Create SparkSession
spark = SparkSession.builder \
    .appName("Transakcije") \
    .getOrCreate()

# Step 2: Configure PostgreSQL connection details
database_properties = {
    "user": "postgres",
    "password": "Rea123Teo",
    "driver": "org.postgresql.Driver"
}

# Step 3: Load data from PostgreSQL
df = spark.read.jdbc(url=database_url, table="Transakcije", properties=database_properties)
df_vrijeme = spark.read.jdbc(url=warehouse_url, table="Vrijeme", properties=database_properties)

# Step 4: Transform Data - Format Price and Deduplicate
columns_to_check_duplicates = [
    "barkod_id",
    "id_trgovine",
    "kolicina",
    "ukupna_cijena",
    "datum_transakcije",
    "popust"
]

df_duplicates = df.dropDuplicates(subset=columns_to_check_duplicates)
df_transformed = df.withColumn("ukupna_cijena", format_number(df["ukupna_cijena"], 2).cast("double"))
df_transformed = df_transformed.withColumn("popust", regexp_replace("popust", "%", ""))
df_transformed = df_transformed.withColumn("popust", df_transformed["popust"].cast("integer"))

df_transformed = df_transformed.join(
    df_vrijeme,
    (year(df_transformed["datum_transakcije"]) == df_vrijeme["Year"]) &
    (month(df_transformed["datum_transakcije"]) == df_vrijeme["Month"]) &
    (dayofmonth(df_transformed["datum_transakcije"]) == df_vrijeme["Day"]),
    "left_outer"
)

#df_transformed = df_transformed.drop("datum_transakcije")
df_transformed.show()

# Select the relevant columns
df_transformed = df_transformed.select("id_transakcije", "barkod_id", "id_trgovine", "id_vrijeme", "kolicina", "ukupna_cijena", "popust")

# Connect to PostgreSQL
connection = psycopg2.connect(**db_params)
cursor = connection.cursor()

# Define the new table name
new_table_name = "Transakcije"

drop_table_sql_transakcije = f"DROP TABLE IF EXISTS {new_table_name};"
cursor.execute(drop_table_sql_transakcije)
connection.commit()

create_table_sql = f"""
    CREATE TABLE {new_table_name} (
        id_transakcije INTEGER PRIMARY KEY,
        barkod_id INTEGER REFERENCES Proizvodi(barkod_id),
        id_trgovine INTEGER REFERENCES Trgovine(id_trgovine),
        id_vrijeme INTEGER REFERENCES Vrijeme(id_vrijeme),
        kolicina INTEGER NOT NULL,
        ukupna_cijena DOUBLE PRECISION NOT NULL,
        popust INTEGER NOT NULL
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
df_transformed.write.jdbc(url=warehouse_url, table=new_table_name, mode="append", properties=database_properties)

# Step 6: Execute ETL Job
spark.stop()
