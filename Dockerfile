FROM python:3.8

#RUN mkdir /app

WORKDIR /app

# Install wait-for-it
ADD https://github.com/vishnubob/wait-for-it/raw/master/wait-for-it.sh /usr/local/bin/wait-for-it
RUN chmod +x /usr/local/bin/wait-for-it

COPY requirements.txt .
COPY postgresql-42.7.1.jar .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install Faker
RUN pip install faker-commerce
RUN pip install psycopg2-binary

#RUN apt-get update && \
 #   apt-get install -y default-jdk && \
  #  java -version
#RUN apt-get update && \
#    apt-get install -y default-jdk && \
#    java -version

COPY . .

#ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
#ENV PATH $JAVA_HOME/bin:$PATH
#ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
#ENV PATH $JAVA_HOME/bin:$PATH

#CMD ["wait-for-it", "postgres:5432", "--", "python", "PostgreSQL/create_transakcije.py"]
CMD ["wait-for-it", "postgres_1:5432", "--timeout=20", "--", "python", "PostgreSQL/create_transakcije.py"]
#CMD ["sh", "-c", "wait-for-it postgres:5432 -- python PostgreSQL/create_transakcije.py && python 'Apache Spark'/spark_session.py"]
#CMD ["sh", "-c", "wait-for-it postgres:5432 -- python PostgreSQL/create_transakcije.py && /usr/bin/java -cp '/usr/local/lib/python3.8/site-packages/pyspark/jars/*' 'Apache Spark'/spark_session.py"]
#CMD ["sh", "-c", "wait-for-it postgres:5432 -- python PostgreSQL/create_transakcije.py && python 'Apache Spark'/spark_session.py"]
