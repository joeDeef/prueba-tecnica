import pandas as pd
from datetime import datetime

def run_etl_patient(df, engine):
    
    print("\nIniciando ETL: dim_patient (Nube)...")
    # Definir unicidad
    df_unique = df[['name', 'gender', 'blood_type']].drop_duplicates().reset_index(drop=True)

    # Consultar existentes para evitar duplicados
    try:
        query_existentes = 'SELECT "name", "gender", "blood_type" FROM "dim_patient"'
        existentes = pd.read_sql(query_existentes, engine)
        set_existentes = set(zip(existentes['name'], existentes['gender'], existentes['blood_type']))
        
        # Filtrar solo los que NO están en la nube
        nuevos = df_unique[~df_unique.apply(
            lambda x: (x['name'], x['gender'], x['blood_type']) in set_existentes, axis=1
        )].copy()
    except Exception:
        nuevos = df_unique.copy()

    if not nuevos.empty:
        # Obtener el último ID
        try:
            query_max = 'SELECT MAX("id_patient") FROM "dim_patient"'
            res = pd.read_sql(query_max, engine).iloc[0, 0]
            last_id = int(res) if res is not None else 0
        except Exception:
            last_id = 0

        # Preparar datos
        nuevos['id_patient'] = range(last_id + 1, last_id + 1 + len(nuevos))
        nuevos['fecha_carga'] = datetime.now()
        nuevos['active'] = 1

        # Cargar a Supabase
        nuevos.to_sql('dim_patient', engine, if_exists='append', index=False, method='multi')
        print(f"Se insertaron {len(nuevos)} pacientes nuevos.")
    else:
        print("No hay pacientes nuevos para insertar.")