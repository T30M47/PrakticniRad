import subprocess

# Define the container name
container_name = "spark"

# Define the commands as lists of arguments
commands = [
    ["docker", "exec", container_name, "spark-submit", "--master", "local[*]", "--driver-class-path", "/app/postgresql-42.7.1.jar", "/app/Apache Spark/spark_proizvodi_2.py"],
    ["docker", "exec", container_name, "spark-submit", "--master", "local[*]", "--driver-class-path", "/app/postgresql-42.7.1.jar", "/app/Apache Spark/spark_trgovine_2.py"],
    ["docker", "exec", container_name, "spark-submit", "--master", "local[*]", "--driver-class-path", "/app/postgresql-42.7.1.jar", "/app/Apache Spark/spark_vrijeme_2.py"],
    ["docker", "exec", container_name, "spark-submit", "--master", "local[*]", "--driver-class-path", "/app/postgresql-42.7.1.jar", "/app/Apache Spark/spark_transakcije_2.py"],
    ["docker", "exec", container_name, "spark-submit", "--master", "local[*]", "--driver-class-path", "/app/postgresql-42.7.1.jar", "/app/Skladiste/create_skladiste.py"],
    ["docker", "exec", container_name, "spark-submit", "--master", "local[*]", "--driver-class-path", "/app/postgresql-42.7.1.jar", "/app/Apache Spark/spark_skladiste.py"],
    ["docker", "exec", container_name, "spark-submit", "--master", "local[*]", "--driver-class-path", "/app/postgresql-42.7.1.jar", "/app/Apache Spark/spark_zalihe.py"]
]

# Run the commands sequentially
for command in commands:
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {' '.join(command)}")
        print(f"Error details: {e}")
        break
