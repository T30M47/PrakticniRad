from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, from_unixtime, when, to_date
from pyspark.sql.types import DateType
import psycopg2
from psycopg2 import sql

# PostgreSQL connection details
db_params_skladiste = {
    "host": "postgres_3",
    "port": 5432,
    "user": "postgres",
    "password": "Rea123Teo",
    "database": "skladiste"
}

# PostgreSQL connection details
db_params_warehouse = {
    "host": "postgres_2",
    "port": 5432,
    "user": "postgres",
    "password": "Rea123Teo",
    "database": "warehouse"
}

# Step 1: Create SparkSession
spark = SparkSession.builder \
    .appName("Zalihe") \
    .getOrCreate()

# Step 2: Configure PostgreSQL connection details
database_url_warehouse = "jdbc:postgresql://postgres_2:5432/warehouse"
database_url_skladiste = "jdbc:postgresql://postgres_3:5432/skladiste"
database_properties = {
    "user": "postgres",
    "password": "Rea123Teo",
    "driver": "org.postgresql.Driver"
}

# Step 3: Load data from PostgreSQL
table_name = "Zalihe"
df_zalihe = spark.read.jdbc(url=database_url_skladiste, table=table_name, properties=database_properties)

df_zalihe = df_zalihe.withColumn("formatted_datum_proizvodnje_temp", 
                                when(unix_timestamp(col("datum_proizvodnje"), "yyyy-MM-dd").isNotNull(),
                                     from_unixtime(unix_timestamp(col("datum_proizvodnje"), "yyyy-MM-dd"), "dd-MM-yyyy"))
                               .otherwise(col("datum_proizvodnje")))

df_zalihe = df_zalihe.withColumn("formatted_datum_isteka_temp", 
                                when(unix_timestamp(col("datum_isteka"), "yyyy-MM-dd").isNotNull(),
                                     from_unixtime(unix_timestamp(col("datum_isteka"), "yyyy-MM-dd"), "dd-MM-yyyy"))
                               .otherwise(col("datum_isteka")))

df_zalihe = df_zalihe.withColumn("formatted_datum_proizvodnje", 
                                when(unix_timestamp(col("formatted_datum_proizvodnje_temp"), "dd/MM/yyyy").isNotNull(),
                                     from_unixtime(unix_timestamp(col("formatted_datum_proizvodnje_temp"), "dd/MM/yyyy"), "dd-MM-yyyy"))
                               .otherwise(col("formatted_datum_proizvodnje_temp")))

df_zalihe = df_zalihe.withColumn("formatted_datum_isteka", 
                                when(unix_timestamp(col("formatted_datum_isteka_temp"), "dd/MM/yyyy").isNotNull(),
                                     from_unixtime(unix_timestamp(col("formatted_datum_isteka_temp"), "dd/MM/yyyy"), "dd-MM-yyyy"))
                               .otherwise(col("formatted_datum_isteka_temp")))
# Drop the temporary columns
df_zalihe = df_zalihe.drop("datum_proizvodnje", "datum_isteka", "formatted_datum_proizvodnje_temp", "formatted_datum_isteka_temp")

# Rename the formatted columns to the original names
df_zalihe = df_zalihe \
    .withColumnRenamed("formatted_datum_proizvodnje", "datum_proizvodnje") \
    .withColumnRenamed("formatted_datum_isteka", "datum_isteka")

df_zalihe = df_zalihe.select("id_zalihe", "barkod_id", "id_skladista", "datum_proizvodnje", "datum_isteka", "dostupna_kolicina")

# Specify the table name to write to
write_table_name = "Zalihe_formatted"

# Write the DataFrame back to the PostgreSQL database
df_zalihe.write.jdbc(url=database_url_skladiste, table=write_table_name, mode="overwrite", properties=database_properties)

spark.stop()


spark = SparkSession.builder.appName("JoinTables").getOrCreate()

# Step 2: Configure PostgreSQL connection details
database_url_warehouse = "jdbc:postgresql://postgres_2:5432/warehouse"
database_url_skladiste = "jdbc:postgresql://postgres_3:5432/skladiste"
database_properties = {
    "user": "postgres",
    "password": "Rea123Teo",
    "driver": "org.postgresql.Driver"
}

# Step 3: Load data from PostgreSQL
table_name = "Proizvodi"
skladiste_df = spark.read.jdbc(url=database_url_skladiste, table=table_name, properties=database_properties)
warehouse_df = spark.read.jdbc(url=database_url_warehouse, table=table_name, properties=database_properties)

# Rename columns in the warehouse_df
skladiste_df = skladiste_df.withColumnRenamed("barkod_id", "barkod_id_skladiste")

# Join the DataFrames on common columns
joined_df = warehouse_df.join(
    skladiste_df,
    (warehouse_df["naziv_proizvoda"] == skladiste_df["naziv_proizvoda"]) &
    (warehouse_df["cijena"] == skladiste_df["cijena"]) &
    (warehouse_df["proizvodjac"] == skladiste_df["proizvodjac"]) &
    (warehouse_df["kategorija"] == skladiste_df["kategorija"]),
    "inner"
)

# Select the desired columns
result_df = joined_df.select(
    warehouse_df["barkod_id"],
    warehouse_df["naziv_proizvoda"],
    warehouse_df["cijena"],
    warehouse_df["proizvodjac"],
    warehouse_df["kategorija"],
    skladiste_df["barkod_id_skladiste"].alias("new_barkod_id")
)

warehouse_url = "jdbc:postgresql://postgres_2:5432/warehouse"

result_df.write.jdbc(url=warehouse_url, table="Proizvodi_temp", mode="overwrite", properties=database_properties)

# Stop the Spark session
spark.stop()

# Create a Spark session
spark = SparkSession.builder.appName("Zalihe").getOrCreate()

# Step 2: Configure PostgreSQL connection details
database_url_warehouse = "jdbc:postgresql://postgres_2:5432/warehouse"
database_url_skladiste = "jdbc:postgresql://postgres_3:5432/skladiste"
database_properties = {
    "user": "postgres",
    "password": "Rea123Teo",
    "driver": "org.postgresql.Driver"
}

# Step 3: Load data from PostgreSQL
table_name = "Zalihe_formatted"
zalihe_df = spark.read.jdbc(url=database_url_skladiste, table=table_name, properties=database_properties)
table_name = "Proizvodi_temp"
proizvodi_df = spark.read.jdbc(url=database_url_warehouse, table=table_name, properties=database_properties)

# Add new_barkod_id column to Zalihe using the existing values in barkod_id from Proizvodi
zalihe_df = zalihe_df.join(proizvodi_df, zalihe_df["barkod_id"] == proizvodi_df["new_barkod_id"], "left").select(zalihe_df["*"], proizvodi_df["barkod_id"].alias("barkod_id_proizvodi"))

# Update barkod_id to be equal to new_barkod_id where they are not the same
zalihe_df = zalihe_df.withColumn("barkod_id", when(col("barkod_id") != col("barkod_id_proizvodi"), col("barkod_id_proizvodi")).otherwise(col("barkod_id")))

# Drop the temporary columns used for the update
zalihe_df = zalihe_df.drop("barkod_id_proizvodi")

zalihe_df = zalihe_df.withColumn("datum_isteka", to_date(col("datum_isteka"), "dd-MM-yyyy").cast(DateType()))

connection = psycopg2.connect(**db_params_warehouse)
cursor = connection.cursor()

# Drop the existing table
drop_table_sql = "DROP TABLE IF EXISTS Zalihe;"
cursor.execute(drop_table_sql)
connection.commit()

create_table_sql = f"""
    CREATE TABLE Zalihe (
        id_zalihe INTEGER PRIMARY KEY,
        barkod_id INTEGER REFERENCES Proizvodi(barkod_id),
        id_skladista INTEGER REFERENCES Skladista(id_skladista),
        datum_proizvodnje VARCHAR(50),
        datum_isteka VARCHAR(50),
        dostupna_kolicina INTEGER
    );
"""

# Execute the SQL statement to create the new table
cursor.execute(create_table_sql)

# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()

zalihe_df.write.jdbc(url=warehouse_url, table="Zalihe", mode="append", properties=database_properties)

# Stop the Spark session
spark.stop()

# Connect to PostgreSQL
connection = psycopg2.connect(**db_params_warehouse)
cursor = connection.cursor()

# SQL command to drop the table if it exists
drop_table_sql = f"DROP TABLE IF EXISTS Proizvodi_temp;"

# Execute the SQL statement to drop the table
cursor.execute(drop_table_sql)

# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()

# Connect to PostgreSQL
connection = psycopg2.connect(**db_params_skladiste)
cursor = connection.cursor()

# SQL command to drop the table if it exists
drop_table_sql = f"DROP TABLE IF EXISTS Zalihe_formatted;"

# Execute the SQL statement to drop the table
cursor.execute(drop_table_sql)

# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()


# Create a Spark session
spark = SparkSession.builder.appName("ZaliheTransakcije").getOrCreate()

# Step 2: Configure PostgreSQL connection details
database_url_warehouse = "jdbc:postgresql://postgres_2:5432/warehouse"
database_url_skladiste = "jdbc:postgresql://postgres_3:5432/skladiste"
database_properties = {
    "user": "postgres",
    "password": "Rea123Teo",
    "driver": "org.postgresql.Driver"
}

# Step 3: Load data from PostgreSQL
table_name = "Zalihe"
zalihe_df = spark.read.jdbc(url=database_url_warehouse, table=table_name, properties=database_properties)
table_name = "Transakcije"
transakcije_df = spark.read.jdbc(url=database_url_warehouse, table=table_name, properties=database_properties)

# Join the DataFrames on common columns
joined_df = zalihe_df.join(
    transakcije_df,
    (zalihe_df["barkod_id"] == transakcije_df["barkod_id"]) &
    (zalihe_df["id_skladista"] == transakcije_df["id_skladista"]),
    "inner"
)

# Select only the desired columns
result_df = joined_df.select("id_transakcije", transakcije_df["barkod_id"], "id_trgovine", "id_vrijeme", transakcije_df["id_skladista"], zalihe_df["id_zalihe"], "kolicina", "ukupna_cijena", "popust")

connection = psycopg2.connect(**db_params_warehouse)
cursor = connection.cursor()

# Drop the existing table
drop_table_sql = "DROP TABLE IF EXISTS Transakcije_new;"
cursor.execute(drop_table_sql)
connection.commit()

create_table_sql = f"""
    CREATE TABLE Transakcije_new (
        id_transakcije INTEGER PRIMARY KEY,
        barkod_id INTEGER REFERENCES Proizvodi(barkod_id),
        id_trgovine INTEGER REFERENCES Trgovine(id_trgovine),
        id_vrijeme INTEGER REFERENCES Vrijeme(id_vrijeme),
        id_skladista INTEGER REFERENCES Skladista(id_skladista),
        id_zalihe INTEGER REFERENCES Zalihe(id_zalihe),
        kolicina INTEGER NOT NULL,
        ukupna_cijena VARCHAR(10) NOT NULL,
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

result_df.write.jdbc(url=warehouse_url, table="Transakcije_new", mode="overwrite", properties=database_properties)

# Stop the Spark session
spark.stop()

# Connect to PostgreSQL
connection = psycopg2.connect(**db_params_warehouse)
cursor = connection.cursor()

# Define the table name to be deleted
table_name_to_delete = "Transakcije"

# SQL command to drop the table if it exists
drop_table_sql = f"DROP TABLE IF EXISTS {table_name_to_delete};"

# Execute the SQL statement to drop the table
cursor.execute(drop_table_sql)

# Commit the changes
connection.commit()

# Define the current and new table names
current_table_name = "Transakcije_new"
new_table_name = "Transakcije"

# SQL command to rename the table
rename_table_sql = f"ALTER TABLE {current_table_name} RENAME TO {new_table_name};"

# Execute the SQL statement to rename the table
cursor.execute(rename_table_sql)

# Commit the changes
connection.commit()

table_name = "Zalihe"
columns_to_delete = ["barkod_id", "id_skladista"]

# Generate the SQL statement to delete columns
"""alter_table_query = sql.SQL("ALTER TABLE {} DROP COLUMN {}").format(
    sql.Identifier(table_name),
    sql.SQL(', ').join(sql.Identifier(column) for column in columns_to_delete)
)"""

columns_str = ", ".join(columns_to_delete)
sql_query = f"ALTER TABLE {table_name} DROP COLUMN {columns_str};"

# Execute the SQL statement
cursor.execute(sql_query)

# Commit the changes to the database
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()
