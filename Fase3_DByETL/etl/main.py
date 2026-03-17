import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Imports de tus módulos de ETL
from etl_dim_time import run_etl_time
from etl_dim_patient import run_etl_patient
from etl_generic import run_etl_generic_dim
from etl_fact_admissions import run_etl_fact

# Import de los Adaptadores
from db_adapter import PostgresAdapter, SQLiteAdapter

def main():
    # Cargar variables de entorno
    load_dotenv()
    
    # MENÚ INTERACTIVO EN CONSOLA
    print("==========================================")
    print("   PIPELINE ETL - GESTIÓN HOSPITALARIA    ")
    print("==========================================")
    print("Seleccione el motor de base de datos destino:")
    print("1. SQLite (Local - db/datawarehouse.db)")
    print("2. PostgreSQL (Nube - Supabase)")
    print("3. Salir")
    
    opcion = input("\nIngrese una opción (1-3): ")

    if opcion == "1":
        # Configuración para SQLite
        db_url = "sqlite:///../db/datawarehouse.db"
        db_name = "SQLite (Local)"
    elif opcion == "2":
        # Configuración para Postgres (desde .env)
        db_url = os.getenv("DATABASE_URL")
        db_name = "PostgreSQL (Supabase)"
        if not db_url:
            print("Error: No se encontró DATABASE_URL en el archivo .env")
            return
    elif opcion == "3":
        print("Saliendo, buen dia")
        return
    else:
        print("Opción no válida.")
        return

    # Configuración de rutas de archivos
    base_path = os.path.dirname(os.path.abspath(__file__))
    DATA_PATH = os.path.join(base_path, "..", "data", "healthcare_dataset_clean.csv")

    # Inicializar motor y Adapter según la elección
    engine = create_engine(db_url)
    adapter = SQLiteAdapter(engine) if opcion == "1" else PostgresAdapter(engine)

    try:
        print(f"\nIniciando proceso en: {db_name}")
        
        if not os.path.exists(DATA_PATH):
            print(f"Error: No se encontró el archivo CSV en {DATA_PATH}")
            return
            
        df = pd.read_csv(DATA_PATH)

        # 5. Pipeline de ejecución
        run_etl_time(df, adapter)
        run_etl_patient(df, adapter)
        
        # Dimensiones simples
        run_etl_generic_dim(df, adapter, 'doctor', 'dim_doctor', 'name', 'id_doctor')
        run_etl_generic_dim(df, adapter, 'hospital', 'dim_hospital', 'name', 'id_hospital')
        run_etl_generic_dim(df, adapter, 'insurance_provider', 'dim_insurance_provider', 'name', 'id_insurance')
        run_etl_generic_dim(df, adapter, 'medical_condition', 'dim_medical_condition', 'medical_condition', 'id_medical_condition')
        
        # Tabla de Hechos
        run_etl_fact(df, adapter)

        print(f"\n¡Éxito! Pipeline completado en {db_name}.")

    except Exception as e:
        print(f"Error crítico: {e}")
        
    finally:
        engine.dispose()
        print(f"Conexión a {db_name} cerrada.")

if __name__ == "__main__":
    main()