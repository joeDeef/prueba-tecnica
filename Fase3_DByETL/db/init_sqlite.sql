-- ============================================================
-- DATA WAREHOUSE: datawarehouse.db
-- Modelo estrella dimensional - Admisiones hospitalarias
-- SQLite >= 3.31.0
-- ============================================================

PRAGMA foreign_keys = ON;

-- ------------------------------------------------------------
-- DIMENSIONES
-- ------------------------------------------------------------

CREATE TABLE dim_time (
  id_time            INTEGER PRIMARY KEY,
  day                INTEGER NOT NULL,
  month              INTEGER NOT NULL,
  month_name         TEXT    NOT NULL,
  year               INTEGER NOT NULL,
  semester           INTEGER NOT NULL,
  quarter            INTEGER NOT NULL,
  four_month_period  INTEGER NOT NULL,
  fecha_carga        TEXT    NOT NULL,
  active             INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE dim_patient (
  id_patient   INTEGER PRIMARY KEY,
  name         TEXT    NOT NULL,
  gender       TEXT    NOT NULL,
  blood_type   TEXT    NOT NULL,
  fecha_carga  TEXT    NOT NULL,
  active       INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE dim_doctor (
  id_doctor    INTEGER PRIMARY KEY,
  name         TEXT    NOT NULL,
  fecha_carga  TEXT    NOT NULL,
  active       INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE dim_hospital (
  id_hospital  INTEGER PRIMARY KEY,
  name         TEXT    NOT NULL,
  fecha_carga  TEXT    NOT NULL,
  active       INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE dim_insurance_provider (
  id_insurance  INTEGER PRIMARY KEY,
  name          TEXT    NOT NULL,
  fecha_carga   TEXT    NOT NULL,
  active        INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE dim_medical_condition (
  id_medical_condition  INTEGER PRIMARY KEY,
  medical_condition     TEXT    NOT NULL,
  fecha_carga           TEXT    NOT NULL,
  active                INTEGER NOT NULL DEFAULT 1
);

-- ------------------------------------------------------------
-- FACT TABLE
-- ------------------------------------------------------------

CREATE TABLE fact_admissions (
  id_admissions          INTEGER PRIMARY KEY,
  sk_date_admision       INTEGER NOT NULL,
  sk_date_discharge      INTEGER NOT NULL,
  sk_patient             INTEGER NOT NULL,
  sk_doctor              INTEGER NOT NULL,
  sk_hospital            INTEGER NOT NULL,
  sk_insurance_provider  INTEGER NOT NULL,
  sk_medical_condition   INTEGER NOT NULL,

  -- medidas
  age_at_admission       INTEGER NOT NULL,
  age_group              TEXT    NOT NULL CHECK (age_group IN (
                           'Lactante', 'Primera infancia', 'Niño',
                           'Adolescente', 'Adulto joven', 'Adulto',
                           'Adulto mayor', 'Sin clasificar'
                         )),
  day_of_stay            INTEGER NOT NULL,
  billing_amount         REAL    NOT NULL,

  -- desnormalizados
  billing_nature         TEXT    NOT NULL CHECK (billing_nature IN ('Ajuste', 'Ingreso')),
  admission_type         TEXT    NOT NULL CHECK (admission_type IN ('Elective', 'Emergency', 'Urgent')),
  medication             TEXT    NOT NULL CHECK (medication IN ('Aspirin', 'Ibuprofen', 'Lipitor', 'Paracetamol', 'Penicillin')),
  test_results           TEXT    NOT NULL CHECK (test_results IN ('Normal', 'Abnormal', 'Inconclusive')),

  fecha_carga            TEXT    NOT NULL,
  active                 INTEGER NOT NULL DEFAULT 1,

  FOREIGN KEY (sk_date_admision)      REFERENCES dim_time(id_time),
  FOREIGN KEY (sk_date_discharge)     REFERENCES dim_time(id_time),
  FOREIGN KEY (sk_patient)            REFERENCES dim_patient(id_patient),
  FOREIGN KEY (sk_doctor)             REFERENCES dim_doctor(id_doctor),
  FOREIGN KEY (sk_hospital)           REFERENCES dim_hospital(id_hospital),
  FOREIGN KEY (sk_insurance_provider) REFERENCES dim_insurance_provider(id_insurance),
  FOREIGN KEY (sk_medical_condition)  REFERENCES dim_medical_condition(id_medical_condition)
);

-- ------------------------------------------------------------
-- ÍNDICES para acelerar los JOINs más frecuentes
-- ------------------------------------------------------------

CREATE INDEX idx_fact_date_admision  ON fact_admissions(sk_date_admision);
CREATE INDEX idx_fact_date_discharge ON fact_admissions(sk_date_discharge);
CREATE INDEX idx_fact_patient        ON fact_admissions(sk_patient);
CREATE INDEX idx_fact_medical        ON fact_admissions(sk_medical_condition);
CREATE INDEX idx_fact_test_results   ON fact_admissions(test_results);
CREATE INDEX idx_time_year_month     ON dim_time(year, month);