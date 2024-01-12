import psycopg2
from psycopg2 import sql
from faker import Faker
import faker_commerce
import random

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
        barkod_id = faker.ean(length = 8)
        naziv_proizvoda = faker.ecommerce_name()
        cijena = round(random.uniform(10.00, 10000.00), 2)
        proizvodjac = generate_variation_of_company(faker.company())
        kategorija = generate_product_category()

        # Insert row
        insert_data_query = """
            INSERT INTO Proizvodi (barkod_id, naziv_proizvoda, cijena, proizvodjac, kategorija)
            VALUES (%s, %s, %s, %s, %s);
        """
        inserted_products.add(naziv_proizvoda)
        cursor.execute(insert_data_query, (barkod_id, naziv_proizvoda, cijena, proizvodjac, kategorija))

        if len(inserted_products) == 500 or len(inserted_products) == 600:
                      cursor.execute(insert_data_query, (random.randint(10000000, 99999999), naziv_proizvoda, cijena, proizvodjac, kategorija))

    print("Data inserted in Proizvodi!")
    conn.commit()

def generate_random_price():
    format_choices = ["{:.0f}", "{:.2f}", "{:.2f}", "{:.2f}"]
    selected_format = random.choice(format_choices)
    return float(selected_format.format(random.uniform(10, 1000)))

def generate_variation_of_company(company_name):
    variations = [
        company_name,
        f"{company_name} Ltd.",
        f"{company_name} Co."
    ]
    return random.choice(variations)

def generate_product_category():
    product_categories = ["Electronics", "Clothing", "Home and Garden", "Beauty", "Books", "Toys", "Sports", "Shoes"]
    return random.choice(product_categories)


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
create_fake_proizvodi()
cursor.execute(create_Trgovine_table)
cursor.execute(create_Transakcije_table)

# Commit the changes and close the connection
conn.commit()
cursor.close()
conn.close()

print("Tables created successfully.")
