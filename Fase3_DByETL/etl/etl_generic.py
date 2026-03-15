import pandas as pd
from datetime import datetime

"""
    ETL Genérico robusto.
    csv_column: Columna en el CSV (ej. 'doctor')
    table_name: Tabla en DB (ej. 'dim_doctor')
    db_column:  Columna de datos en DB (ej. 'name' o 'medical_condition')
    pk_name:    Nombre exacto de la llave primaria (ej. 'id_doctor')
"""
def run_etl_generic_dim(df, engine, csv_column, table_name, db_column, pk_name):

    print(f"\nIniciando ETL: {table_name} (Nube)...")
    
    # Extraer únicos y limpiar
    df_unique = df[[csv_column]].drop_duplicates().dropna()
    df_unique.columns = [db_column]

    # Consultar valores existentes para evitar duplicados
    try:
        # Usamos comillas dobles para que Postgres no ignore las mayúsculas
        query_existentes = f'SELECT "{db_column}" FROM "{table_name}"'
        existentes = pd.read_sql(query_existentes, engine)[db_column].values
        nuevos = df_unique[~df_unique[db_column].isin(existentes)].copy()
    except Exception:
        nuevos = df_unique.copy()

    if not nuevos.empty:
        # Obtener el último ID
        try:
            query_max = f'SELECT MAX("{pk_name}") FROM "{table_name}"'
            res = pd.read_sql(query_max, engine).iloc[0, 0]
            last_id = int(res) if res is not None else 0
        except Exception:
            last_id = 0

        # Asignar ID y metadata
        nuevos[pk_name] = range(last_id + 1, last_id + 1 + len(nuevos))
        nuevos['fecha_carga'] = datetime.now()
        nuevos['active'] = 1

        # Carga a la DB en la nube
        nuevos.to_sql(table_name, engine, if_exists='append', index=False, method='multi')
        print(f"Se insertaron {len(nuevos)} registros en {table_name}.")
    else:
        print(f"{table_name} ya está actualizado.")