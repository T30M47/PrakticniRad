import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import psycopg2
import pandas as pd

# Create a Dash web app
app = dash.Dash(__name__)

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

# Close the database connection
conn.close()

# Define the layout of your Dash app
app.layout = html.Div(children=[
    html.H1(children='Dash Materialized Views Example'),

    dcc.Dropdown(
        id='warehouse-dropdown',
        options=[{'label': location, 'value': location} for location in warehouse_locations],
        value=warehouse_locations[0],  # Set the default value to the first warehouse location
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
    )
])

# Define callback to update the graph based on the selected warehouse location
@app.callback(
    Output('example-graph', 'figure'),
    [Input('warehouse-dropdown', 'value')]
)
def update_graph(selected_warehouse):
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

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050)
