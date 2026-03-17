import pandas as pd
from datetime import datetime

def run_etl_patient(df, adapter):
    """
    Proceso ETL para la dimensión de pacientes. 
    Usa el adapter para manejar la persistencia y asegurar la unicidad.
    """
    print("\nIniciando ETL: dim_patient...")

    # Definir claves naturales que identifican a un paciente único
    natural_keys = ['name', 'gender', 'blood_type']

    # Limpieza local: Quitar duplicados del DataFrame de entrada
    df_unique = df[natural_keys].drop_duplicates().reset_index(drop=True)

    # Consultar la base de datos a través del adapter
    existentes_df = adapter.get_existing_data('dim_patient', natural_keys)

    # Convertimos los existentes en un set de tuplas para una búsqueda instantánea
    set_existentes = set(zip(
        existentes_df['name'], 
        existentes_df['gender'], 
        existentes_df['blood_type']
    ))

    # Identificar quiénes son realmente nuevos
    # (Aquellos cuya combinación de nombre, género y sangre NO esté en la DB)
    nuevos = df_unique[~df_unique.apply(
        lambda x: (x['name'], x['gender'], x['blood_type']) in set_existentes, axis=1
    )].copy()

    if not nuevos.empty:
        # Generar Surrogate Keys
        # Le pedimos al adapter el último ID para seguir la secuencia
        last_id = adapter.get_max_id('dim_patient', 'id_patient')

        nuevos['id_patient'] = range(last_id + 1, last_id + 1 + len(nuevos))
        
        # Añadir variables de control
        nuevos['fecha_carga'] = datetime.now()
        nuevos['active'] = 1

        # Carga final mediante el adapter
        adapter.insert_dataframe(nuevos, 'dim_patient')
        
        print(f"Éxito: Se insertaron {len(nuevos)} pacientes nuevos.")
    else:
        print("No hay pacientes nuevos para insertar (Idempotencia verificada).")