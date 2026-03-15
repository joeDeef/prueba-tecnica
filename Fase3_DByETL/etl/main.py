import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Imports de tus módulos
from etl_dim_time import run_etl_time
from etl_dim_patient import run_etl_patient
from etl_generic import run_etl_generic_dim
from etl_fact_admissions import run_etl_fact

def main():
    load_dotenv()
    
    # Configuración de conexión a la base de datos
    db_url = os.getenv("DATABASE_URL")
    
    # Configuración de rutas
    base_path = os.path.dirname(os.path.abspath(__file__))
    DATA_PATH = os.path.join(base_path, "..", "data", "healthcare_dataset_clean.csv")

    # Crear motor de conexión
    engine = create_engine(db_url)

    try:
        print("Conectando a Supabase mediante URI...")
        df = pd.read_csv(DATA_PATH)

        # Pipeline de ejecución
        run_etl_time(df, engine)
        run_etl_patient(df, engine)
        
        # Dimensiones simples
        run_etl_generic_dim(df, engine, 'doctor', 'dim_doctor', 'name', 'id_doctor')
        run_etl_generic_dim(df, engine, 'hospital', 'dim_hospital', 'name', 'id_hospital')
        run_etl_generic_dim(df, engine, 'insurance_provider', 'dim_insurance_provider', 'name', 'id_insurance')
        run_etl_generic_dim(df, engine, 'medical_condition', 'dim_medical_condition', 'medical_condition', 'id_medical_condition')
        
        # Tabla de Hechos
        run_etl_fact(df, engine)

        print("\nCarga completa en Supabase.")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        engine.dispose()

if __name__ == "__main__":
    main()