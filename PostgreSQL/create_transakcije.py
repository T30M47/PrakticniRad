import psycopg2
from psycopg2 import sql

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="postgres",
    port=5432,
    user="postgres",
    password="Rea123Teo",
    database="transakcije"
)

# Create a cursor
cursor = conn.cursor()

# Create tables
create_Proizvodi_table = """
CREATE TABLE IF NOT EXISTS Proizvodi (
    barkod_id INTEGER PRIMARY KEY,
    naziv_proizvoda VARCHAR(255) NOT NULL,
    cijena NUMERIC(10, 2) NOT NULL,
    proizvodjac VARCHAR(255),
    kategorija VARCHAR(255) 
);
"""

create_Trgovine_table = """
CREATE TABLE IF NOT EXISTS Trgovine (
    id_trgovine INTEGER PRIMARY KEY,
    naziv_trgovine VARCHAR(255) NOT NULL,
    lokacija VARCHAR(255) NOT NULL
);
"""

create_Transakcije_table = """
CREATE TABLE IF NOT EXISTS Transakcije (
    id_transakcije INTEGER PRIMARY KEY,
    barkod_id INTEGER REFERENCES Proizvodi(barkod_id),
    id_trgovine INTEGER REFERENCES Trgovine(id_trgovine),
    kolicina INTEGER NOT NULL,
    ukupna_cijena NUMERIC(10, 2) NOT NULL,
    popust INTEGER
);
"""

# Execute table creation queries
cursor.execute(create_Proizvodi_table)
cursor.execute(create_Trgovine_table)
cursor.execute(create_Transakcije_table)

# Commit the changes and close the connection
conn.commit()
conn.close()

print("Tables created successfully.")
