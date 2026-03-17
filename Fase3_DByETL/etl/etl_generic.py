import pandas as pd
from datetime import datetime

"""
    ETL Genérico usando el patrón Adapter.
    csv_column: Columna en el CSV (ej. 'doctor')
    table_name: Tabla en DB (ej. 'dim_doctor')
    db_column:  Columna de datos en DB (ej. 'name')
    pk_name:    Nombre exacto de la llave primaria (ej. 'id_doctor')
"""
def run_etl_generic_dim(df, adapter, csv_column, table_name, db_column, pk_name):

    print(f"\nIniciando ETL: {table_name}...")
    
    # Extraer únicos y limpiar localmente
    df_unique = df[[csv_column]].drop_duplicates().dropna()
    df_unique.columns = [db_column]

    # Consultar valores existentes a través del ADAPTER
    # Usamos get_existing_ids pasándole la columna del nombre/categoría
    existentes = adapter.get_existing_ids(table_name, db_column)
    
    # Filtrar solo los que no están en la DB
    nuevos = df_unique[~df_unique[db_column].isin(existentes)].copy()

    if not nuevos.empty:
        # Obtener el último ID mediante el ADAPTER
        last_id = adapter.get_max_id(table_name, pk_name)

        # Asignar ID correlativo y metadata
        nuevos[pk_name] = range(last_id + 1, last_id + 1 + len(nuevos))
        nuevos['fecha_carga'] = datetime.now()
        nuevos['active'] = 1

        # Carga a la DB mediante el ADAPTER
        adapter.insert_dataframe(nuevos, table_name)
        print(f"Se insertaron {len(nuevos)} registros en {table_name}.")
    else:
        print(f"{table_name} ya está actualizado.")