# Data Warehouse - Procesos ETL

Transformación de datos brutos de salud en información accionable mediante un pipeline ETL completo con modelo estrella dimensional.

## Autor

**Jose Defaz**
josejoel.defaz@gmail.com

---

## Stack tecnológico

| Capa | Tecnología |
|------|-----------|
| Lenguaje | Python 3.x |
| Transformación | Pandas |
| Base de datos nube | PostgreSQL vía Supabase |
| Conexión BD | SQLAlchemy + Psycopg2 |
| Variables de entorno | Python-dotenv |

---

## Instalación y ejecución

### 1. Crear entorno virtual

```bash
python -m venv venv
source venv/bin/activate        # Linux / Mac
venv\Scripts\activate           # Windows
```

### 2. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 3. Configurar variables de entorno

Crear un archivo `.env` en la raíz del proyecto:

```env
DATABASE_URL=postgresql://postgres:[PASSWORD]@[HOST]:[PORT]/postgres
```

> Si no se configura `.env`, el pipeline detecta automáticamente la ausencia de conexión

### 4. Ejecutar el pipeline

```bash
cd ./etl/
python main.py
```

El orquestador ejecuta las etapas Extract → Transform → Load en orden.

> El pipeline es **idempotente**: ejecutarlo más de una vez no duplica datos.

---

## Estructura del proyecto

```
proyecto/
├── data/
│   └── healthcare_dataset.csv
├── db/
│   ├── init_sqlite.sql          # DDL para SQLite
│   └── init_postgresql.sql      # DDL para PostgreSQL / Supabase
├── docs/
├── etl/
│   ├── etl_dim_patient.py
│   ├── etl_dim_time.py
│   ├── etl_fact_admissions.py
│   ├── etl_generic.py
│   └── main.py
├── requirements.txt
├── .env
```

---

## Base de datos

Se mantienen dos versiones del DDL para distintos entornos:

| Archivo | Motor | Uso |
|---------|-------|-----|
| [`db/init_sqlite.sql`](db/init_sqlite.sql) | SQLite | Desarrollo local, sin servidor |
| [`db/init_postgresql.sql`](db/init_postgresql.sql) | PostgreSQL | Producción en Supabase |

**¿Por qué dos motores?**

Se comenzó con SQLite por portabilidad: un solo archivo, sin servidor, sin instalación. Sin embargo, para conectar herramientas de visualización como Power BI o Looker Studio se requieren drivers adicionales y configuración manual. Supabase expone PostgreSQL directamente en la nube, lo que permite conectar cualquier herramienta de BI con solo la cadena de conexión.

Para inicializar la base local en SQLite:

```bash
cd db/
python ./int_db.py
sqlite3 db/datawarehouse.db < db/init_sqlite.sql
```

Para inicializar en PostgreSQL desde Supabase, ejecutar el contenido de `db/init_postgresql.sql` desde el SQL Editor del dashboard.

---