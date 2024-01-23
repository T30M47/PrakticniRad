from pyspark.sql import SparkSession
import psycopg2

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
    .appName("Skladista") \
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
table_name = "Transakcije"
df_transakcije = spark.read.jdbc(url=database_url_warehouse, table=table_name, properties=database_properties)
table_name = "Trgovine"
df_trgovine = spark.read.jdbc(url=database_url_warehouse, table=table_name, properties=database_properties)
table_name = "Skladista"
df_skladiste = spark.read.jdbc(url=database_url_skladiste, table=table_name, properties=database_properties)

# Connect to PostgreSQL
connection = psycopg2.connect(**db_params_warehouse)
cursor = connection.cursor()

# Define the new table name
new_table_name = "Skladista"

drop_table_sql_transakcije = f"DROP TABLE IF EXISTS {new_table_name};"
cursor.execute(drop_table_sql_transakcije)
connection.commit()

create_table_sql = f"""
    CREATE TABLE {new_table_name} (
        id_skladista INTEGER PRIMARY KEY,
        lokacija VARCHAR(255) NOT NULL,
        CONSTRAINT unique_id_skladista UNIQUE (id_skladista)
    )
"""

# Execute the SQL statement to create the new table
cursor.execute(create_table_sql)

# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()

table_name = "Skladista"
df_skladiste.write.jdbc(url=database_url_warehouse, table=table_name, mode="append", properties=database_properties)

# Register DataFrames as temporary tables
df_skladiste.createOrReplaceTempView("skladista")
df_transakcije.createOrReplaceTempView("transakcije")
df_trgovine.createOrReplaceTempView("trgovine")

result = df_transakcije \
    .join(df_trgovine, df_transakcije["id_trgovine"] == df_trgovine["id_trgovine"]) \
    .join(df_skladiste, df_trgovine["lokacija"] == df_skladiste["lokacija"]) \
    .select(df_transakcije["*"], df_skladiste["id_skladista"].alias("id_skladista"))

connection = psycopg2.connect(**db_params_warehouse)
cursor = connection.cursor()

# Drop the existing table
drop_table_sql = "DROP TABLE IF EXISTS Transakcije_skladiste;"
cursor.execute(drop_table_sql)
connection.commit()

create_table_sql = f"""
    CREATE TABLE Transakcije_skladiste (
        id_transakcije INTEGER PRIMARY KEY,
        barkod_id INTEGER REFERENCES Proizvodi(barkod_id),
        id_trgovine INTEGER REFERENCES Trgovine(id_trgovine),
        id_vrijeme INTEGER REFERENCES Vrijeme(id_vrijeme),
        kolicina INTEGER NOT NULL,
        ukupna_cijena VARCHAR(10) NOT NULL,
        popust INTEGER NOT NULL,
        id_skladista INTEGER REFERENCES Skladista(id_skladista)
    )
"""

# Execute the SQL statement to create the new table
cursor.execute(create_table_sql)

# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()

result.write.jdbc(url=database_url_warehouse, table="Transakcije_skladiste", mode="append", properties=database_properties)

# Step 7: Stop SparkSession
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
current_table_name = "Transakcije_skladiste"
new_table_name = "Transakcije"

# SQL command to rename the table
rename_table_sql = f"ALTER TABLE {current_table_name} RENAME TO {new_table_name};"

# Execute the SQL statement to rename the table
cursor.execute(rename_table_sql)

# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()