# Fase 1 — Exploración y Diagnóstico del Dataset

Este documento resume los hallazgos del análisis exploratorio (EDA) realizado sobre el dataset hospitalario `healthcare_dataset.csv`, cubriendo calidad de datos, validaciones, transformaciones y observaciones clave.

---

## 📋 Descripción General del Dataset

| Atributo | Valor |
|---|---|
| **Total de registros** | 55,500 |
| **Total de columnas** | 15 |
| **Tipos de datos** | `float64` (1), `int64` (2), `object` (12) |

### Columnas disponibles

| Columna | Tipo | Descripción |
|---|---|---|
| `Name` | object | Nombre del paciente |
| `Age` | int64 | Edad del paciente |
| `Gender` | object | Género |
| `Blood Type` | object | Tipo de sangre |
| `Medical Condition` | object | Condición médica |
| `Date of Admission` | object → datetime | Fecha de ingreso |
| `Doctor` | object | Médico tratante |
| `Hospital` | object | Hospital |
| `Insurance Provider` | object | Proveedor de seguro |
| `Billing Amount` | float64 | Monto facturado |
| `Room Number` | int64 | Número de habitación |
| `Admission Type` | object | Tipo de admisión |
| `Discharge Date` | object → datetime | Fecha de alta |
| `Medication` | object | Medicamento recetado |
| `Test Results` | object | Resultado del examen |

---

## 🔍 Fase 1: Calidad de Datos

### 1.1 Nulos y Blancos

> **Sin problemas.** Todas las columnas tienen **0 valores nulos y 0 valores en blanco** (0% de datos faltantes). Se realizó doble verificación con `.isnull()` y detección de strings vacíos/espacios.

### 1.2 Duplicados

> **534 filas duplicadas encontradas.**

- Todos los duplicados son **registros exactamente idénticos en todas sus columnas**.
- La distribución es uniforme: cada grupo duplicado aparece exactamente **2 veces** (534 pares).
- **Acción recomendada:** `df.drop_duplicates(keep='first')`

**Ejemplo de duplicados encontrados:**

| Name | Age | Date of Admission | Doctor | Billing Amount |
|---|---|---|---|---|
| ABIgaIL YOung | 41 | 2022-12-15 | Edward Kramer | 1,983.57 |
| ABIgaIL YOung | 41 | 2022-12-15 | Edward Kramer | 1,983.57 |

---

## 📊 Fase 2: Cardinalidad y Valores Únicos

| Columna | Valores Únicos | Observación |
|---|---|---|
| `Name` | 49,992 | Alta cardinalidad — inconsistencias de formato (mayúsculas/minúsculas mezcladas) |
| `Age` | 77 | Rango de 13 a 89 años |
| `Gender` | 2 | `Male`, `Female` — perfectamente balanceado |
| `Blood Type` | 8 | Los 8 grupos estándar (`A+`, `A-`, `B+`, `B-`, `AB+`, `AB-`, `O+`, `O-`) |
| `Medical Condition` | 6 | `Arthritis`, `Asthma`, `Cancer`, `Diabetes`, `Hypertension`, `Obesity` |
| `Date of Admission` | 1,827 | Rango desde `2019-05-08` |
| `Doctor` | 40,341 | **Cardinalidad extremadamente alta** — casi un doctor único por paciente |
| `Hospital` | 39,876 | **Cardinalidad extremadamente alta** — casi un hospital único por paciente |
| `Insurance Provider` | 5 | `Aetna`, `Blue Cross`, `Cigna`, `Medicare`, `UnitedHealthcare` |
| `Billing Amount` | 50,000 | Casi único por registro — incluye **valores negativos** |
| `Room Number` | 400 | Del 101 al 500 |
| `Admission Type` | 3 | `Elective`, `Emergency`, `Urgent` |
| `Discharge Date` | 1,856 | Coherente con `Date of Admission` |
| `Medication` | 5 | `Aspirin`, `Ibuprofen`, `Lipitor`, `Paracetamol`, `Penicillin` |
| `Test Results` | 3 | `Normal`, `Abnormal`, `Inconclusive` |

### 🔑 Observaciones de Cardinalidad

- `Doctor` y `Hospital` tienen cardinalidad de ~40K únicos en 55K filas — **no representan entidades reutilizables** y su valor analítico directo es limitado. Apunta a datos sintéticos generados con nombres aleatorios.
- `Gender`, `Blood Type`, `Medical Condition`, `Insurance Provider`, `Medication` y `Test Results` están **altamente balanceadas**, comportamiento típico de **datasets sintéticos o generados artificialmente**.

---

## 📅 Fase 3: Validación de Fechas e Ingeniería de Características

### Nueva variable: `days_of_stay`

Se creó calculando la diferencia entre `Discharge Date` y `Date of Admission`:

```python
df['days_of_stay'] = (df['discharge_date'] - df['date_of_admission']).dt.days
```

| Métrica | Valor |
|---|---|
| Mínimo | **1 día** |
| Máximo | **30 días** |
| Valores únicos | 30 (del 1 al 30, sin saltos ni huecos) |

> **Sin registros negativos.** No existe ningún caso donde la fecha de alta sea anterior a la de ingreso — integridad temporal confirmada.

---

## 💰 Fase 4: Análisis de `Billing Amount`

| Estadístico | Valor |
|---|---|
| Mínimo | **-2,008.49** |
| Percentil 25 | 13,241.22 |
| Mediana | 25,538.07 |
| Promedio | 25,539.32 |
| Percentil 75 | 37,820.51 |

> Se detectaron **valores negativos** en `Billing Amount`. No se consideran errores — son plausiblemente **ajustes contables, reembolsos o créditos** a favor del paciente. Se recomienda documentarlos como categoría propia en la capa de transformación.

---

## 🛠️ Fase 5: Transformaciones Aplicadas

| Transformación | Detalle |
|---|---|
| **Normalización de columnas** | Todos los nombres convertidos a `snake_case` (minúsculas, sin espacios) |
| **Conversión de fechas** | `Date of Admission` y `Discharge Date` de `object` → `datetime64[ns]` |
| **Codificación numérica temporal** | Variables categóricas codificadas con `cat.codes` solo para visualización de histogramas |
| **Nueva variable** | `days_of_stay` calculada como diferencia entre fechas |

### ⚠️ Inconsistencias de formato en texto

La columna `Name` presenta mezcla aleatoria de mayúsculas y minúsculas (ej: `"ABIgaIL YOung"`, `"adrIENNE bEll"`). **Acción recomendada:** aplicar `.str.title()` en el pipeline de limpieza.

---

## 📈 Valores Atípicos (Outliers)

Los histogramas generados para todas las variables muestran:

- **No se detectaron outliers representativos** en `age`, `room_number` ni `days_of_stay`.
- `Billing Amount` es la única variable con dispersión notable debido a los valores negativos.
- Las distribuciones uniformes de las variables categóricas refuerzan la hipótesis de **dataset sintético y balanceado artificialmente**.

---

## 🔗 Archivos del Proyecto

| Nombre | Tipo | Enlace / Ubicación | Cloud|
| :--- | :--- | :--- |:--- |
| **Notebook de Trabajo** | 📓 Jupyter Notebook | [`./PruebaTécnica_DataEngineerIntern.ipynb`](./PruebaTécnica_DataEngineerIntern.ipynb) | [Abrir en Google Colab](https://colab.research.google.com/drive/1VKUBwPElwSPr4QK_9rUOPkjW1YL0jMi7?usp=sharing) |
| **Dataset Descagado** | 🗄️ CSV | [`healthcare_dataset.csv`](./healthcare_dataset.csv) | [Abrir en Kaggle](https://www.kaggle.com/datasets/prasad22/healthcare-dataset) |