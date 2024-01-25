import dash
import dash_core_components as dcc
import dash_html_components as html
import psycopg2
import pandas as pd

# Create a Dash web app
app = dash.Dash(__name__)

# Define the connection parameters for your PostgreSQL warehouse
db_params = {
    'host': 'postgres_2',  # Use the service name from your Docker Compose file
    'database': 'warehouse',
    'user': 'postgres',
    'password': 'Rea123Teo',
}

# Connect to PostgreSQL
conn = psycopg2.connect(**db_params)

# Define the query to find products with smallest supply by different warehouses
query = """
SELECT
    Zalihe.id_zalihe,
    Proizvodi.naziv_proizvoda,
    Zalihe.dostupna_kolicina AS total_quantity,
    Skladista.lokacija AS warehouse_location
FROM
    Zalihe
JOIN
    Transakcije ON Zalihe.id_zalihe = Transakcije.id_zalihe
JOIN
    Proizvodi ON Transakcije.barkod_id = Proizvodi.barkod_id
JOIN
    Skladista ON Transakcije.id_skladista = Skladista.id_skladista
WHERE
    Zalihe.dostupna_kolicina < 100;
"""

# Fetch data from the query
df = pd.read_sql_query(query, conn)

# Close the database connection
conn.close()

# Define the layout of your Dash app
app.layout = html.Div(children=[
    html.H1(children='Dash Materialized Views Example'),

    dcc.Graph(
        id='example-graph',
        figure={
            'data': [
                {'x': df['naziv_proizvoda'], 'y': df['total_quantity'], 'type': 'bar', 'name': 'Products with Smallest Supply'},
            ],
            'layout': {
                'title': 'Products with Smallest Supply by Different Warehouses'
            }
        }
    )
])

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050)
