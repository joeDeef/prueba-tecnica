import pandas as pd
import numpy as np
from datetime import datetime

meses_completos = {
    1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 
    5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto', 
    9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
}

def run_etl_time(df, adapter):
    print("\nIniciando ETL para dim_time...")
    
    # 1. Transformación
    fechas = pd.concat([
        pd.to_datetime(df['date_of_admission']).dt.normalize(), 
        pd.to_datetime(df['discharge_date']).dt.normalize()
    ]).unique()
    
    dim_time_df = pd.DataFrame(fechas, columns=['full_date'])
    dim_time_df['id_time'] = dim_time_df['full_date'].dt.strftime('%Y%m%d').astype(int)
    dim_time_df['day'] = dim_time_df['full_date'].dt.day
    dim_time_df['month'] = dim_time_df['full_date'].dt.month
    dim_time_df['month_name'] = dim_time_df['month'].map(meses_completos)
    dim_time_df['year'] = dim_time_df['full_date'].dt.year
    dim_time_df['semester'] = np.where(dim_time_df['month'] <= 6, 1, 2)
    dim_time_df['quarter'] = dim_time_df['full_date'].dt.quarter
    dim_time_df['four_month_period'] = np.ceil(dim_time_df['month'] / 4).astype(int)
    dim_time_df['fecha_carga'] = datetime.now()
    dim_time_df['active'] = 1

    # Carga usando el ADAPTER
    existentes = adapter.get_existing_ids('dim_time', 'id_time')
    nuevos_registros = dim_time_df[~dim_time_df['id_time'].isin(existentes)].copy()

    if not nuevos_registros.empty:
        # Solo las columnas que están en la DB
        cols = ['id_time', 'day', 'month', 'month_name', 'year', 
                'semester', 'quarter', 'four_month_period', 'fecha_carga', 'active']
        
        adapter.insert_dataframe(nuevos_registros[cols], 'dim_time')
        print(f"Se insertaron {len(nuevos_registros)} nuevas fechas.")
    else:
        print("No hay fechas nuevas para insertar.")