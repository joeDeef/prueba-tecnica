import pandas as pd
from sqlalchemy import create_engine

class PostgresAdapter:
    def __init__(self, engine):
        self.engine = engine

    def get_existing_ids(self, table_name, id_column):
        """Busca valores únicos en una columna (IDs o Nombres)"""
        try:
            query = f'SELECT "{id_column}" FROM "{table_name}"'
            return set(pd.read_sql(query, self.engine)[id_column].values)
        except Exception:
            return set()

    def get_existing_data(self, table_name, columns):
        """Trae un DataFrame con columnas específicas para mapeos o comparaciones"""
        cols_formatted = ", ".join([f'"{c}"' for c in columns])
        query = f'SELECT {cols_formatted} FROM "{table_name}"'
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            print(f"Advertencia: No se pudo leer {table_name}: {e}")
            return pd.DataFrame(columns=columns)

    def get_max_id(self, table_name, id_column):
        """Obtiene el valor máximo de una columna para continuar la secuencia"""
        query = f'SELECT MAX("{id_column}") FROM "{table_name}"'
        try:
            res = pd.read_sql(query, self.engine).iloc[0, 0]
            return int(res) if res is not None else 0
        except Exception:
            return 0

    def insert_dataframe(self, df, table_name, if_exists='append'):
        """
        Inserta datos de forma eficiente. 
        if_exists: 'append' para acumular o 'replace' para sobreescribir.
        """
        df.to_sql(
            table_name, 
            self.engine, 
            if_exists=if_exists, 
            index=False, 
            method='multi',
            chunksize=1000
        )

class SQLiteAdapter:
    def __init__(self, engine):
        self.engine = engine

    def get_existing_ids(self, table_name, id_column):
        try:
            query = f'SELECT "{id_column}" FROM "{table_name}"'
            return set(pd.read_sql(query, self.engine)[id_column].values)
        except Exception:
            return set()

    def get_existing_data(self, table_name, columns):
        cols_formatted = ", ".join([f'"{c}"' for c in columns])
        query = f'SELECT {cols_formatted} FROM "{table_name}"'
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            return pd.DataFrame(columns=columns)

    def get_max_id(self, table_name, id_column):
        query = f'SELECT MAX("{id_column}") FROM "{table_name}"'
        try:
            res = pd.read_sql(query, self.engine).iloc[0, 0]
            return int(res) if res is not None else 0
        except Exception:
            return 0

    def insert_dataframe(self, df, table_name, if_exists='append'):
        df.to_sql(
            table_name, 
            self.engine, 
            if_exists=if_exists, 
            index=False, 
            method=None,
            chunksize=5000
        )