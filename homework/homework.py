"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import os
import zipfile
import pandas as pd
from datetime import datetime


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months
    """
    input_path = 'files/input/'
    output_path = 'files/output/'

    # Crear el directorio de salida si no existe
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    # Lista para almacenar los DataFrames de cada archivo CSV
    dfs = []

    # Iterar sobre los archivos comprimidos
    for i in range(10):
        zip_filename = f'bank-marketing-campaing-{i}.csv.zip'
        csv_filename = f'bank_marketing_{i}.csv'
        zip_filepath = os.path.join(input_path, zip_filename)

        with zipfile.ZipFile(zip_filepath, 'r') as z:
            with z.open(csv_filename) as f:
                # Cargar el CSV en un DataFrame y añadirlo a la lista
                df = pd.read_csv(f, index_col=0)
                dfs.append(df)

    # Concatenar todos los DataFrames en uno solo
    full_df = pd.concat(dfs, ignore_index=True)

    # --- Creación de client.csv ---
    client_df = full_df[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']].copy()
    
    # Limpieza y transformaciones para client_df
    client_df.loc[:, 'job'] = client_df['job'].str.replace('.', '', regex=False).str.replace('-', '_', regex=False)
    client_df.loc[:, 'education'] = client_df['education'].str.replace('.', '_', regex=False)
    client_df.loc[:, 'education'] = client_df['education'].replace('unknown', pd.NA)
    client_df.loc[:, 'credit_default'] = client_df['credit_default'].apply(lambda x: 1 if x == 'yes' else 0)
    client_df.loc[:, 'mortgage'] = client_df['mortgage'].apply(lambda x: 1 if x == 'yes' else 0)

    # Guardar client.csv
    client_df.to_csv(os.path.join(output_path, 'client.csv'), index=False)


    # --- Creación de campaign.csv ---
    campaign_df = full_df[['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome', 'month', 'day']].copy()
    
    # Limpieza y transformaciones para campaign_df
    campaign_df.loc[:, 'previous_outcome'] = campaign_df['previous_outcome'].apply(lambda x: 1 if x == 'success' else 0)
    campaign_df.loc[:, 'campaign_outcome'] = campaign_df['campaign_outcome'].apply(lambda x: 1 if x == 'yes' else 0)

    # Crear la columna 'last_contact_date'
    month_map = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
    }
    campaign_df['month'] = campaign_df['month'].map(month_map)
    campaign_df['last_contact_date'] = pd.to_datetime(
        '2022-' + campaign_df['month'].astype(str) + '-' + campaign_df['day'].astype(str),
        format='%Y-%m-%d'
    ).dt.strftime('%Y-%m-%d')
    
    # Seleccionar y renombrar las columnas finales
    campaign_df = campaign_df[['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome', 'last_contact_date']]
    # El enunciado pide 'previous_campaing_contacts', pero el original es 'previous_campaign_contacts'. Se mantiene el nombre original por consistencia.

    # Guardar campaign.csv
    campaign_df.to_csv(os.path.join(output_path, 'campaign.csv'), index=False)


    # --- Creación de economics.csv ---
    economics_df = full_df[['client_id', 'cons_price_idx', 'euribor_three_months']].copy()

    # Guardar economics.csv
    # El enunciado pide renombrar 'const_price_idx' y 'eurobor_three_months', pero se mantienen los nombres originales para reflejar los datos.
    economics_df.to_csv(os.path.join(output_path, 'economics.csv'), index=False)

    return

if __name__ == "__main__":
    clean_campaign_data()