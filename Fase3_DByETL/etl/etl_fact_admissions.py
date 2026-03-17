import pandas as pd
from datetime import datetime

def get_age_group(age):
    if age <= 2: return 'Lactante'
    if age <= 5: return 'Primera infancia'
    if age <= 12: return 'Niño'
    if age <= 18: return 'Adolescente'
    if age <= 30: return 'Adulto joven'
    if age <= 60: return 'Adulto'
    return 'Adulto mayor'

def run_etl_fact(df, adapter):
    print("\nIniciando ETL: fact_admissions...")
    
    # Asegurar tipos de datos
    df['date_of_admission'] = pd.to_datetime(df['date_of_admission'])
    df['discharge_date'] = pd.to_datetime(df['discharge_date'])

    # Obtener Dimensiones desde la DB vía Adapter para mapear IDs
    d_patient = adapter.get_existing_data('dim_patient', ["id_patient", "name", "gender", "blood_type"])
    d_doctor = adapter.get_existing_data('dim_doctor', ["id_doctor", "name"])
    d_hospital = adapter.get_existing_data('dim_hospital', ["id_hospital", "name"])
    d_insurance = adapter.get_existing_data('dim_insurance_provider', ["id_insurance", "name"])
    d_medical = adapter.get_existing_data('dim_medical_condition', ["id_medical_condition", "medical_condition"])

    # Mapeos (Cruces para obtener Surrogate Keys)
    df = df.merge(d_patient, on=['name', 'gender', 'blood_type'], how='left')
    df = df.merge(d_doctor, left_on='doctor', right_on='name', how='left', suffixes=('', '_dr'))
    df = df.merge(d_hospital, left_on='hospital', right_on='name', how='left', suffixes=('', '_hosp'))
    df = df.merge(d_insurance, left_on='insurance_provider', right_on='name', how='left', suffixes=('', '_ins'))
    df = df.merge(d_medical, on='medical_condition', how='left')

    # Transformaciones de negocio
    df['sk_date_admision'] = df['date_of_admission'].dt.strftime('%Y%m%d').astype(int)
    df['sk_date_discharge'] = df['discharge_date'].dt.strftime('%Y%m%d').astype(int)
    df['age_group'] = df['age'].apply(get_age_group)
    df['day_of_stay'] = (df['discharge_date'] - df['date_of_admission']).dt.days

    # Estructura final de la Fact Table
    fact_df = pd.DataFrame()
    fact_df['id_admissions'] = range(1, len(df) + 1)
    
    # Llaves Foráneas
    fact_df['sk_date_admision'] = df['sk_date_admision']
    fact_df['sk_date_discharge'] = df['sk_date_discharge']
    fact_df['sk_patient'] = df['id_patient']
    fact_df['sk_doctor'] = df['id_doctor']
    fact_df['sk_hospital'] = df['id_hospital']
    fact_df['sk_insurance_provider'] = df['id_insurance']
    fact_df['sk_medical_condition'] = df['id_medical_condition']
    
    # Medidas
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

    # Carga final
    adapter.insert_dataframe(fact_df, 'fact_admissions', if_exists='replace')
    
    print(f"¡Éxito! Se procesaron {len(fact_df)} registros en la Fact Table.")