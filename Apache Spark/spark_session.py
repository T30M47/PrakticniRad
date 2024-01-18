"""from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("YourETLJob") \
    .config("spark.jars", "/app/postgresql-42.7.1.jar") \
    .getOrCreate()"""

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

# Create a window specification to partition by proizvodjac
#window_spec = Window().partitionBy("proizvodjac")

# Add "Co." to companies without "Co." based on the window
df_transformed = df_transformed.withColumn(
    "proizvodjac",
    F.when(F.col("proizvodjac").endswith("Co."), F.col("proizvodjac"))
    .otherwise(F.concat(F.col("proizvodjac"), F.lit(" Co.")))
)


#spark.sql(f"ALTER TABLE {new_table_name} ADD PRIMARY KEY (barkod_id)")

# Step 5: Write the transformed data to the new table in warehouse database
#warehouse_url = "jdbc:postgresql://prakticnirad_postgres_2_1:5432/warehouse"
df_transformed.write.jdbc(url=warehouse_url, table=table_name, mode="append", properties=database_properties)

#df_transformed.write.jdbc(url=database_url, table=new_table_name, mode="overwrite", properties=database_properties)

# Step 6: Execute ETL Job
spark.stop()


# Step 1: Create SparkSession
spark = SparkSession.builder \
    .appName("Trgovine") \
    .getOrCreate()


# Step 2: Configure PostgreSQL connection details
database_url = "jdbc:postgresql://prakticnirad_postgres_1:5432/transakcije"
database_properties = {
    "user": "postgres",
    "password": "Rea123Teo",
    "driver": "org.postgresql.Driver"
}

# Step 3: Load data from PostgreSQL
table_name = "Trgovine"
df = spark.read.jdbc(url=database_url, table=table_name, properties=database_properties)

# Step 4: Transform Data - Format Price and Deduplicate
columns_to_check_duplicates = ["naziv_trgovine", "lokacija"]
df_transformed = df.dropDuplicates(subset=columns_to_check_duplicates)

#spark.sql(f"ALTER TABLE {new_table_name} ADD PRIMARY KEY (barkod_id)")

# Connect to PostgreSQL
connection = psycopg2.connect(**db_params)
cursor = connection.cursor()

# Define the new table name
new_table_name = "Trgovine"

drop_table_sql_trgovine = f"DROP TABLE IF EXISTS {new_table_name};"
cursor.execute(drop_table_sql_trgovine)
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

# Step 5: Write the transformed data to the new table in warehouse database
#warehouse_url = "jdbc:postgresql://prakticnirad_postgres_2_1:5432/warehouse"
df_transformed.write.jdbc(url=warehouse_url, table=table_name, mode="append", properties=database_properties)

#df_transformed.write.jdbc(url=database_url, table=new_table_name, mode="overwrite", properties=database_properties)

# Step 6: Execute ETL Job
spark.stop()


# Step 2: Configure PostgreSQL connection details
database_url = "jdbc:postgresql://prakticnirad_postgres_1:5432/transakcije"
database_properties = {
    "user": "postgres",
    "password": "Rea123Teo",
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("Vrijeme") \
    .getOrCreate()

# Step 3: Load data from PostgreSQL
table_name = "Transakcije"
df = spark.read.jdbc(url=database_url, table=table_name, properties=database_properties)

df_deduplicated = df.dropDuplicates(["datum_transakcije"])

df_transformed = df_deduplicated.withColumn("Year", year("datum_transakcije")) \
    .withColumn("Month", month("datum_transakcije")) \
    .withColumn("Day", dayofmonth("datum_transakcije"))  \
    .withColumn("DayOfWeek", dayofweek("datum_transakcije") - 1)

# Step 5: Add id_vrijeme column
window_spec = Window.orderBy("datum_transakcije")
df_transformed = df_transformed.withColumn("id_vrijeme", F.row_number().over(window_spec))

# Step 6: Select relevant columns, including the new columns for year, month, day, day of week, and id_vrijeme
df_export = df_transformed.select("id_vrijeme", "Year", "Month", "Day", "DayOfWeek")

table_name = "Vrijeme"

# Connect to PostgreSQL
connection = psycopg2.connect(**db_params)
cursor = connection.cursor()

drop_table_sql_vrijeme= f"DROP TABLE IF EXISTS {table_name};"
cursor.execute(drop_table_sql_vrijeme)
connection.commit()

create_table_sql = f"""
    CREATE TABLE {table_name} (
        id_vrijeme INTEGER PRIMARY KEY,
        Year INTEGER,
        Month INTEGER,
        Day INTEGER,
        DayOfWeek INTEGER
    )
"""

# Execute the SQL statement to create the new table
cursor.execute(create_table_sql)

# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()

# Write the DataFrame to the warehouse table
df_export.write.jdbc(url=warehouse_url, table=table_name, mode="append", properties=database_properties)

# Stop the Spark session
spark.stop()



# Step 1: Create SparkSession
spark = SparkSession.builder \
    .appName("Transakcije") \
    .getOrCreate()

# Step 2: Configure PostgreSQL connection details
database_url = "jdbc:postgresql://prakticnirad_postgres_1:5432/transakcije"
database_properties = {
    "user": "postgres",
    "password": "Rea123Teo",
    "driver": "org.postgresql.Driver"
}

# Step 3: Load data from PostgreSQL
table_name = "Transakcije"
df = spark.read.jdbc(url=database_url, table=table_name, properties=database_properties)

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


df_vrijeme = spark.read.jdbc(url=warehouse_url, table="Vrijeme", properties=database_properties)

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

# Define the SQL statement to create the new table
"""create_table_sql = f
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

#spark.sql(f"ALTER TABLE {new_table_name} ADD PRIMARY KEY (barkod_id)")

# Step 5: Write the transformed data to the new table in warehouse database
#warehouse_url = "jdbc:postgresql://prakticnirad_postgres_2_1:5432/warehouse"
df_transformed.write.jdbc(url=warehouse_url, table=new_table_name, mode="append", properties=database_properties)

#df_transformed.write.jdbc(url=database_url, table=new_table_name, mode="overwrite", properties=database_properties)

# Step 6: Execute ETL Job
spark.stop()

