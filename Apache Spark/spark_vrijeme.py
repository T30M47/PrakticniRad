# from pyspark.sql import SparkSession
# from pyspark.sql.functions import year, month, dayofweek, dayofmonth
# import psycopg2
# from pyspark.sql.window import Window
# from pyspark.sql import functions as F


# # PostgreSQL connection details
# db_params = {
#     "host": "prakticnirad_postgres_2_1",
#     "port": 5432,
#     "user": "postgres",
#     "password": "Rea123Teo",
#     "database": "warehouse"
# }

# warehouse_url = "jdbc:postgresql://prakticnirad_postgres_2_1:5432/warehouse"
# database_url = "jdbc:postgresql://prakticnirad_postgres_1:5432/transakcije"

# # Step 2: Configure PostgreSQL connection details
# database_properties = {
#     "user": "postgres",
#     "password": "Rea123Teo",
#     "driver": "org.postgresql.Driver"
# }

# spark = SparkSession.builder \
#     .appName("Vrijeme") \
#     .getOrCreate()

# # Step 3: Load data from PostgreSQL
# table_name = "Transakcije"
# df = spark.read.jdbc(url=database_url, table=table_name, properties=database_properties)

# df_deduplicated = df.dropDuplicates(["datum_transakcije"])

# df_transformed = df_deduplicated.withColumn("Year", year("datum_transakcije")) \
#     .withColumn("Month", month("datum_transakcije")) \
#     .withColumn("Day", dayofmonth("datum_transakcije"))  \
#     .withColumn("DayOfWeek", dayofweek("datum_transakcije") - 1)

# # Step 5: Add id_vrijeme column
# window_spec = Window.orderBy("datum_transakcije")
# df_transformed = df_transformed.withColumn("id_vrijeme", F.row_number().over(window_spec))

# # Step 6: Select relevant columns, including the new columns for year, month, day, day of week, and id_vrijeme
# df_export = df_transformed.select("id_vrijeme", "Year", "Month", "Day", "DayOfWeek")

# table_name = "Vrijeme"

# # Connect to PostgreSQL
# connection = psycopg2.connect(**db_params)
# cursor = connection.cursor()

# drop_table_sql_vrijeme= f"DROP TABLE IF EXISTS {table_name};"
# cursor.execute(drop_table_sql_vrijeme)
# connection.commit()

# create_table_sql = f"""
#     CREATE TABLE {table_name} (
#         id_vrijeme INTEGER PRIMARY KEY,
#         Year INTEGER,
#         Month INTEGER,
#         Day INTEGER,
#         DayOfWeek INTEGER
#     )
# """

# # Execute the SQL statement to create the new table
# cursor.execute(create_table_sql)

# # Commit the changes
# connection.commit()

# # Close the cursor and connection
# cursor.close()
# connection.close()

# # Write the DataFrame to the warehouse table
# df_export.write.jdbc(url=warehouse_url, table=table_name, mode="append", properties=database_properties)

# # Stop the Spark session
# spark.stop()

