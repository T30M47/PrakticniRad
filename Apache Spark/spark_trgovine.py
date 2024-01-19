# from pyspark.sql import SparkSession
# import psycopg2


# # PostgreSQL connection details
# db_params = {
#     "host": "prakticnirad_postgres_2_1",
#     "port": 5432,
#     "user": "postgres",
#     "password": "Rea123Teo",
#     "database": "warehouse"
# }

# warehouse_url = "jdbc:postgresql://prakticnirad_postgres_2_1:5432/warehouse"


# # Step 1: Create SparkSession
# spark = SparkSession.builder \
#     .appName("Trgovine") \
#     .getOrCreate()


# # Step 2: Configure PostgreSQL connection details
# database_url = "jdbc:postgresql://prakticnirad_postgres_1:5432/transakcije"
# database_properties = {
#     "user": "postgres",
#     "password": "Rea123Teo",
#     "driver": "org.postgresql.Driver"
# }

# # Step 3: Load data from PostgreSQL
# table_name = "Trgovine"
# df = spark.read.jdbc(url=database_url, table=table_name, properties=database_properties)

# # Step 4: Transform Data - Format Price and Deduplicate
# columns_to_check_duplicates = ["naziv_trgovine", "lokacija"]
# df_transformed = df.dropDuplicates(subset=columns_to_check_duplicates)

# # Connect to PostgreSQL
# connection = psycopg2.connect(**db_params)
# cursor = connection.cursor()

# # Define the new table name
# new_table_name = "Trgovine"

# drop_table_sql_trgovine = f"DROP TABLE IF EXISTS {new_table_name};"
# cursor.execute(drop_table_sql_trgovine)
# connection.commit()

# # Define the SQL statement to create the new table
# create_table_sql = f"""
#     CREATE TABLE {new_table_name} (
#         id_trgovine INTEGER PRIMARY KEY,
#         naziv_trgovine VARCHAR(255) NOT NULL,
#         lokacija VARCHAR(255) NOT NULL,
#         CONSTRAINT unique_id_trgovine UNIQUE (id_trgovine)
#     )
# """

# # Execute the SQL statement to create the new table
# cursor.execute(create_table_sql)

# # Commit the changes
# connection.commit()

# # Close the cursor and connection
# cursor.close()
# connection.close()

# # Step 5: Write the transformed data to the new table in warehouse database
# df_transformed.write.jdbc(url=warehouse_url, table=table_name, mode="append", properties=database_properties)

# # Step 6: Execute ETL Job
# spark.stop()