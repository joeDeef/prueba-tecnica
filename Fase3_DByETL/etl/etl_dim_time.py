import pandas as pd
import numpy as np
from datetime import datetime

# Diccionario de meses
meses_completos = {
        1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 
        5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto', 
        9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
    }

def run_etl_time(df, engine):
        
    print("\nIniciando ETL para dim_time (Nube)...")
    # Asegurar que las columnas sean datetime
    date_cols = ['date_of_admission', 'discharge_date']
    for col in date_cols:
        if not pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col])

    # Extraer fechas únicas de ambas columnas y normalizar (quitar horas)
    fechas_admision = df['date_of_admission'].dt.normalize()
    fechas_alta = df['discharge_date'].dt.normalize()
    todas_las_fechas = pd.concat([fechas_admision, fechas_alta]).unique()
    
    # Crear DataFrame de la dimensión
    dim_time_df = pd.DataFrame(todas_las_fechas, columns=['full_date'])

    # Generar Atributos
    # id_time en formato YYYYMMDD (Ej: 20240115)
    dim_time_df['id_time'] = dim_time_df['full_date'].dt.strftime('%Y%m%d').astype(int)
    dim_time_df['day'] = dim_time_df['full_date'].dt.day
    dim_time_df['month'] = dim_time_df['full_date'].dt.month
    dim_time_df['month_name'] = dim_time_df['month'].map(meses_completos)
    dim_time_df['year'] = dim_time_df['full_date'].dt.year
    dim_time_df['semester'] = np.where(dim_time_df['month'] <= 6, 1, 2)
    dim_time_df['quarter'] = dim_time_df['full_date'].dt.quarter
    dim_time_df['four_month_period'] = np.ceil(dim_time_df['month'] / 4).astype(int)
    
    # Metadata: PostgreSQL prefiere objetos datetime nativos
    dim_time_df['fecha_carga'] = datetime.now()
    dim_time_df['active'] = 1
    
    # Carga para evitar duplicados
    try:
        # Nota: Usamos comillas dobles por si Postgres es estricto con las mayúsculas/minúsculas
        existentes = pd.read_sql('SELECT "id_time" FROM "dim_time"', engine)['id_time'].values
    except Exception:
        existentes = []

    # Filtrar solo los registros que no están en la DB
    nuevos_registros = dim_time_df[~dim_time_df['id_time'].isin(existentes)].copy()
    
    if not nuevos_registros.empty:
        columnas_sql = [
            'id_time', 'day', 'month', 'month_name', 'year', 
            'semester', 'quarter', 'four_month_period', 'fecha_carga', 'active'
        ]
        
        # Carga a Supabase
        nuevos_registros[columnas_sql].to_sql(
            'dim_time', 
            engine, 
            if_exists='append', 
            index=False, 
            method='multi'
        )
        print(f"Se insertaron {len(nuevos_registros)} nuevas fechas en la nube.")
    else:
        print("No hay fechas nuevas para insertar.")