import psycopg2
from psycopg2 import sql
from faker import Faker
import faker_commerce
import random
import datetime

faker = Faker()
faker.add_provider(faker_commerce.Provider)
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


# Kreiranje tablice Proizvodi:
        # ubaceni duplikati
        # uskladiti nazive kompanija (recimo da svi imaju nastavak Co., ili ako su dvije iste s Co. i jedna bez da se usklade)
def create_fake_proizvodi():
    inserted_products = set()
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
    cursor.execute(create_Proizvodi_table)
    conn.commit()

    # Generate fake data with different variations
    for _ in range(1000):
        while True:
            barkod_id = faker.ean(length = 8)
            if barkod_id not in inserted_products:
                break
        naziv_proizvoda = faker.ecommerce_name()
        cijena = round(random.uniform(10.00, 10000.00), 2)
        proizvodjac = generate_variation_of_company(faker.company())
        kategorija = faker.ecommerce_category()

        # Insert row
        insert_data_query = """
            INSERT INTO Proizvodi (barkod_id, naziv_proizvoda, cijena, proizvodjac, kategorija)
            VALUES (%s, %s, %s, %s, %s);
        """
        inserted_products.add(barkod_id)
        cursor.execute(insert_data_query, (barkod_id, naziv_proizvoda, cijena, proizvodjac, kategorija))

        if len(inserted_products) == 500 or len(inserted_products) == 600:
                      cursor.execute(insert_data_query, (random.randint(10000000, 99999999), naziv_proizvoda, cijena, proizvodjac, kategorija))

    print("Data inserted in Proizvodi!")
    conn.commit()

def generate_random_price():
    format_choices = ["{:.0f}", "{:.1f}", "{:.2f}"]
    selected_format = random.choice(format_choices)
    return float(selected_format.format(random.uniform(10, 1000)))

def generate_variation_of_company(company_name):
    variations = [
        company_name,
        f"{company_name} Ltd.",
        f"{company_name} Co."
    ]
    return random.choice(variations)

def generate_random_popust():
    with_percentage = random.choice([True, False])

    if with_percentage:
        return f"{random.randint(1, 50)}%"
    else:
        return random.randint(1, 50)

trgovine_nazivi = ["Konzum", "Super-Maxi Konzum", "Mini-Konzum", "MultiplusZum", "ExtraKonzum"]
lokacije_nazivi = ["Zagreb", "Rijeka", "Osijek", "Rijeka", "Split"]

# Kreiranje tablice Trgovine:
        # ubaceni duplikati
def create_fake_trgovine():
    inserted_stores = set()

    create_Trgovine_table = """
    CREATE TABLE IF NOT EXISTS Trgovine (
        id_trgovine INTEGER PRIMARY KEY,
        naziv_trgovine VARCHAR(255) NOT NULL,
        lokacija VARCHAR(255) NOT NULL
    );
    """
    cursor.execute(create_Trgovine_table)
    conn.commit()

    # Generate fake data with different variations
    for i in range(5):
        while True:
            id_trgovine = random.randint(10000, 99999)
            if id_trgovine not in inserted_stores:
                break
        inserted_stores.add(id_trgovine)    
        # Insert row
        insert_data_query = """
            INSERT INTO Trgovine (id_trgovine, naziv_trgovine, lokacija)
            VALUES (%s, %s, %s);
        """
        cursor.execute(insert_data_query, (id_trgovine, trgovine_nazivi[i], lokacije_nazivi[i]))

        if len(inserted_stores) == 3:
                cursor.execute(insert_data_query, (random.randint(10000, 99999), trgovine_nazivi[i], lokacije_nazivi[i]))
        
    print("Data inserted in Trgovine!")
    conn.commit()


# Kreiranje tablice Transakcije
        # Ubaceni duplikati
        # uskladiti cijene (da su sve recimo s dvije decimalne tocke)
        # uskladiti popuste (da svi imaju ili nemaju znak za %)
def create_fake_transakcije():
    inserted_transactions = set()

    create_Transakcije_table = """
    CREATE TABLE IF NOT EXISTS Transakcije (
        id_transakcije INTEGER PRIMARY KEY,
        barkod_id INTEGER REFERENCES Proizvodi(barkod_id),
        id_trgovine INTEGER REFERENCES Trgovine(id_trgovine),
        kolicina INTEGER NOT NULL,
        ukupna_cijena DOUBLE PRECISION NOT NULL,
        datum_transakcije DATE NOT NULL,
        popust VARCHAR(5)
    );
    """
    cursor.execute(create_Transakcije_table)
    conn.commit()

    start_date = datetime.date(2020, 1, 1)
    end_date = datetime.date(2022, 12, 31)

    for _ in range(5000):
        while True:
            id_transakcije = random.randint(1000000, 9999999)
            if id_transakcije not in inserted_transactions:
                break
        barkod_id = get_random_barkod_id()
        id_trgovine = get_random_id_trgovine()
        kolicina = random.randint(1, 10)
        ukupna_cijena = generate_random_price()
        datum_transakcije = faker.date_between(start_date=start_date, end_date=end_date)
        popust = generate_random_popust()
        # Insert row
        insert_data_query = """
            INSERT INTO Transakcije (id_transakcije, barkod_id, id_trgovine, kolicina, ukupna_cijena, datum_transakcije, popust)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        inserted_transactions.add(id_transakcije)
        cursor.execute(insert_data_query, (id_transakcije, barkod_id, id_trgovine, kolicina, ukupna_cijena, datum_transakcije, popust))

        if len(inserted_transactions) == 500 or len(inserted_transactions) == 600:
                      cursor.execute(insert_data_query, (random.randint(10000000, 99999999), barkod_id, id_trgovine, kolicina, ukupna_cijena, datum_transakcije, popust))

    print("Data inserted in Transakcije!")
    conn.commit()

def get_random_barkod_id():
    # Function to query and return a valid barkod_id from the Proizvodi table
    cursor.execute("SELECT barkod_id FROM Proizvodi ORDER BY RANDOM() LIMIT 1;")
    result = cursor.fetchone()
    return result[0] if result else None

def get_random_id_trgovine():
    # Function to query and return a valid id_trgovine from the Trgovine table
    cursor.execute("SELECT id_trgovine FROM Trgovine ORDER BY RANDOM() LIMIT 1;")
    result = cursor.fetchone()
    return result[0] if result else None

# Execute table creation queries
create_fake_proizvodi()
create_fake_trgovine()
create_fake_transakcije()

# Commit the changes and close the connection
conn.commit()
cursor.close()
conn.close()

print("Tables created successfully.")
