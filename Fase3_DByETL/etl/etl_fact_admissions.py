import pandas as pd
import numpy as np
from datetime import datetime

def get_age_group(age):
    if age <= 2: return 'Lactante'
    if age <= 5: return 'Primera infancia'
    if age <= 12: return 'Niño'
    if age <= 18: return 'Adolescente'
    if age <= 30: return 'Adulto joven'
    if age <= 60: return 'Adulto'
    return 'Adulto mayor'

def run_etl_fact(df, engine):

    print("\nIniciando ETL: fact_admissions (Subiendo a la nube)...")
    
    # Asegurar tipos de datos de fecha
    df['date_of_admission'] = pd.to_datetime(df['date_of_admission'])
    df['discharge_date'] = pd.to_datetime(df['discharge_date'])

    # Obtener IDs de las Dimensiones desde Supabase
    # Usamos comillas dobles para evitar errores de sensibilidad a mayúsculas en Postgres
    dim_patient = pd.read_sql('SELECT "id_patient", "name", "gender", "blood_type" FROM "dim_patient"', engine)
    dim_doctor = pd.read_sql('SELECT "id_doctor", "name" FROM "dim_doctor"', engine)
    dim_hospital = pd.read_sql('SELECT "id_hospital", "name" FROM "dim_hospital"', engine)
    dim_insurance = pd.read_sql('SELECT "id_insurance", "name" FROM "dim_insurance_provider"', engine)
    dim_medical = pd.read_sql('SELECT "id_medical_condition", "medical_condition" FROM "dim_medical_condition"', engine)

    # MAPEOS (Cruzar CSV con IDs de la DB)

    # Paciente
    df = df.merge(dim_patient, on=['name', 'gender', 'blood_type'], how='left')
    
    # Doctor
    df = df.merge(dim_doctor, left_on='doctor', right_on='name', how='left', suffixes=('', '_dr'))
    
    # Hospital
    df = df.merge(dim_hospital, left_on='hospital', right_on='name', how='left', suffixes=('', '_hosp'))
    
    # Seguro
    df = df.merge(dim_insurance, left_on='insurance_provider', right_on='name', how='left', suffixes=('', '_ins'))
    
    # Condición Médica
    df = df.merge(dim_medical, on='medical_condition', how='left')

    # TRANSFORMACIONES Y MEDIDAS
    df['sk_date_admision'] = df['date_of_admission'].dt.strftime('%Y%m%d').astype(int)
    df['sk_date_discharge'] = df['discharge_date'].dt.strftime('%Y%m%d').astype(int)
    df['age_group'] = df['age'].apply(get_age_group)
    df['day_of_stay'] = (df['discharge_date'] - df['date_of_admission']).dt.days

    # PREPARACIÓN DEL DATAFRAME FINAL
    fact_df = pd.DataFrame()
    fact_df['id_admissions'] = range(1, len(df) + 1)
    
    # Llaves Foráneas (SKs)
    fact_df['sk_date_admision'] = df['sk_date_admision']
    fact_df['sk_date_discharge'] = df['sk_date_discharge']
    fact_df['sk_patient'] = df['id_patient']
    fact_df['sk_doctor'] = df['id_doctor']
    fact_df['sk_hospital'] = df['id_hospital']
    fact_df['sk_insurance_provider'] = df['id_insurance']
    fact_df['sk_medical_condition'] = df['id_medical_condition']
    
    # Medidas y Datos
    fact_df['age_at_admission'] = df['age']
    fact_df['age_group'] = df['age_group']
    fact_df['day_of_stay'] = df['day_of_stay']
    fact_df['billing_amount'] = df['billing_amount']
    fact_df['billing_nature'] = df['billing_nature']
    fact_df['admission_type'] = df['admission_type']
    fact_df['medication'] = df['medication']
    fact_df['test_results'] = df['test_results']
    
    # Metadata
    fact_df['fecha_carga'] = datetime.now()
    fact_df['active'] = 1

    # CARGA A POSTGRESQL (Supabase)
    fact_df.to_sql(
        'fact_admissions', 
        engine, 
        if_exists='replace', 
        index=False, 
        method='multi', 
        chunksize=1000
    )
    
    print(f"¡Éxito! Se cargaron {len(fact_df)} registros en fact_admissions.")