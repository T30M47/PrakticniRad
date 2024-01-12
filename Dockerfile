FROM python:3.8

WORKDIR /app

# Install wait-for-it
ADD https://github.com/vishnubob/wait-for-it/raw/master/wait-for-it.sh /usr/local/bin/wait-for-it
RUN chmod +x /usr/local/bin/wait-for-it

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install Faker
RUN pip install faker-commerce


COPY . .

CMD ["wait-for-it", "postgres:5432", "--", "python", "PostgreSQL/create_transakcije.py"]

