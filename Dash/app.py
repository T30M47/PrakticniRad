import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc  # Make sure to import dash_bootstrap_components

import psycopg2
import pandas as pd

# Create a Dash web app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Define the connection parameters for your PostgreSQL warehouse
db_params = {
    'host': 'postgres_2',
    'database': 'warehouse',
    'user': 'postgres',
    'password': 'Rea123Teo',
}

# Connect to PostgreSQL
conn = psycopg2.connect(**db_params)

# Fetch the unique warehouse locations
warehouse_locations = pd.read_sql_query("SELECT DISTINCT lokacija FROM skladista;", conn)['lokacija'].tolist()

# Fetch the unique stores
store_names = pd.read_sql_query("SELECT DISTINCT naziv_trgovine FROM trgovine;", conn)['naziv_trgovine'].tolist()

# Close the database connection
conn.close()

# Define the layout of your Dash app
app.layout = html.Div(children=[
    html.H1(children='Analitika i upravljanje zalihama'),

    dcc.Dropdown(
        id='warehouse-dropdown',
        options=[{'label': location, 'value': location} for location in warehouse_locations],
        value=warehouse_locations[0],
        multi=False,
        style={'width': '50%'}
    ),

    dcc.Graph(
        id='example-graph',
        figure={
            'data': [],
            'layout': {
                'title': 'Products with Smallest Supply by Different Warehouses'
            }
        }
    ),

    dcc.Dropdown(
        id='store-dropdown',
        options=[{'label': store, 'value': store} for store in store_names],
        value=store_names[0],
        multi=False,
        style={'width': '50%'}
    ),

    dcc.Graph(
        id='average-sales-graph',
        figure={
            'data': [],
            'layout': {
                'title': 'Average Sales per Store in the Last Three Months'
            }
        }
    )
])

# Define callback to update the first graph based on the selected warehouse location
@app.callback(
    Output('example-graph', 'figure'),
    [Input('warehouse-dropdown', 'value')]
)
def update_example_graph(selected_warehouse):
    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_params)

    # Define the query to find products with the smallest supply for the selected warehouse
    materialized_view = f"""
    CREATE MATERIALIZED VIEW low_supply_materialized_view AS
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
        Zalihe.dostupna_kolicina < 200
        AND Skladista.lokacija = '{selected_warehouse}';
    """
    # Execute the materialized views queries
    with conn.cursor() as cursor:
        cursor.execute(materialized_view)

    # Fetch data from the materialized view
    query = """
    SELECT * FROM low_supply_materialized_view;
    """
    # Fetch data from the query
    df = pd.read_sql_query(query, conn)

    # Close the database connection
    conn.close()

    return {
        'data': [
            {'x': df['naziv_proizvoda'], 'y': df['total_quantity'], 'type': 'bar', 'name': 'Products with Smallest Supply'},
        ],
        'layout': {
            'title': f'Products with Smallest Supply at Warehouse: {selected_warehouse}'
        }
    }

# Define callback to update the second graph based on the selected store
@app.callback(
    Output('average-sales-graph', 'figure'),
    [Input('store-dropdown', 'value')]
)
def update_average_sales_graph(selected_store):
    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_params)

    # Define the query to find average sales per store in the last three months of a year
    materialized_view_avg_sales = f"""
    CREATE MATERIALIZED VIEW avg_sales_materialized_view AS
    SELECT
        Trgovine.naziv_trgovine AS store,
        Vrijeme.month,
        AVG(CAST(Transakcije.ukupna_cijena AS NUMERIC)) AS avg_sales
    FROM
        Transakcije
    JOIN
        Vrijeme ON Transakcije.id_vrijeme = Vrijeme.id_vrijeme
    JOIN
        Trgovine ON Transakcije.id_trgovine = Trgovine.id_trgovine
    WHERE
        Vrijeme.month >= 10
        AND Vrijeme.month <= 12
        AND Trgovine.naziv_trgovine = '{selected_store}'
    GROUP BY store, Vrijeme.month;
        """
    # Execute the materialized views queries
    with conn.cursor() as cursor:
        cursor.execute(materialized_view_avg_sales)

    # Fetch data from the materialized view
    query = """
    SELECT * FROM avg_sales_materialized_view;
    """
    # Fetch data from the query
    df_avg_sales = pd.read_sql_query(query, conn)

    # Close the database connection
    conn.close()

    return {
        'data': [
            {'x': df_avg_sales['month'], 'y': df_avg_sales['avg_sales'], 'type': 'bar', 'name': 'Average Sales'},
        ],
        'layout': {
            'title': f'Average Sales per Store in the year last quarter from 2019 - 2025 at Store: {selected_store}'
        }
    }

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050)
