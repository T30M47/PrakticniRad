import psycopg2
from psycopg2 import sql

conn = psycopg2.connect(
    host="postgres",
    port=5432,
    user="postgres",
    password="Rea123Teo",
    database="skladiste"
)

# Create a cursor
cursor = conn.cursor()

create_ProizvodiDim_table = """
    CREATE TABLE IF NOT EXISTS ProizvodiDim (
        barkod_id INTEGER PRIMARY KEY,
        naziv_proizvoda VARCHAR(255) NOT NULL,
        cijena NUMERIC(10, 2) NOT NULL,
        proizvodjac VARCHAR(255),
        kategorija VARCHAR(255) 
    );
    """

create_TrgovineDim_table = """
    CREATE TABLE IF NOT EXISTS TrgovineDim (
        id_trgovine INTEGER PRIMARY KEY,
        naziv_trgovine VARCHAR(255) NOT NULL,
        lokacija VARCHAR(255) NOT NULL
    );
    """

create_VrijemeDim_table = """
    CREATE TABLE IF NOT EXISTS VrijemeDim (
        id_vremena INTEGER PRIMARY KEY,
        dan INTEGER NOT NULL,
        mjesec INTEGER NOT NULL,
        godina INTEGER NOT NULL
    );
    """

create_TransakcijeFact_table = """
    CREATE TABLE IF NOT EXISTS TransakcijeFact (
        barkod_id INTEGER REFERENCES ProizvodiDim(barkod_id),
        id_trgovine INTEGER REFERENCES TrgovineDim(id_trgovine),
        id_vremena INTEGER REFERENCES VrijemeDim(id_vremena),
        kolicina INTEGER NOT NULL,
        ukupna_cijena DOUBLE PRECISION NOT NULL,
        popust VARCHAR(5)
    );
    """

cursor.execute(create_ProizvodiDim_table)
cursor.execute(create_TrgovineDim_table)
cursor.execute(create_TrgovineDim_table)
cursor.execute(create_TransakcijeFact_table)

conn.commit()
cursor.close()
conn.close()

print("Warehouse created successfully.")