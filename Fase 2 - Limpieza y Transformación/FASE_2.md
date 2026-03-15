# Fase 2 — Limpieza y Transformación del Dataset

Este documento detalla todas las decisiones, transformaciones y validaciones aplicadas sobre el dataset hospitalario para producir un archivo limpio y listo para su uso en pipelines de datos.

**Input:** `healthcare_dataset.csv` — 55,500 registros · 15 columnas
**Output:** `healthcare_dataset_clean.csv` — 54,966 registros · 17 columnas

---

## 🗂️ Resumen del Pipeline de Limpieza

| Paso | Acción | Registros afectados |
|:---:|---|---|
| 1 | Normalización de cabeceras a `snake_case` | 15 columnas renombradas |
| 2 | Eliminación de duplicados exactos | -534 filas |
| 3 | Estandarización de texto (`str.title()`) | Columnas `name`, `doctor`, `hospital` |
| 4 | Re-validación de duplicados ocultos | 0 adicionales encontrados |
| 5 | Conversión de fechas a `datetime64` | 2 columnas (`date_of_admission`, `discharge_date`) |
| 6 | Ingeniería de características: `days_of_stay` | +1 columna nueva |
| 7 | Tratamiento de `billing_amount` negativos | +1 columna nueva `billing_nature` |
| 8 | Validación final de integridad | Sin errores |

---

## Paso 1 — Normalización de Cabeceras

Se cargó una copia del dataset original (`df_limpio = df_original.copy()`) para preservar los datos crudos y trabajar sobre la copia.

Todos los nombres de columnas fueron convertidos a `snake_case`:

```python
df_limpio.columns = df_limpio.columns.str.strip().str.lower().str.replace(' ', '_')
```

**Justificación:** Evita errores de sintaxis en Python, facilita el acceso a columnas y es el estándar en pipelines de datos modernos.

| Antes | Después |
|---|---|
| `Name` | `name` |
| `Date of Admission` | `date_of_admission` |
| `Blood Type` | `blood_type` |
| `Medical Condition` | `medical_condition` |
| `Insurance Provider` | `insurance_provider` |
| `Billing Amount` | `billing_amount` |
| `Room Number` | `room_number` |
| `Admission Type` | `admission_type` |
| `Discharge Date` | `discharge_date` |
| `Test Results` | `test_results` |

---

## Paso 2 — Eliminación de Duplicados Exactos

```python
df_limpio = df_limpio.drop_duplicates(keep='first')
```

| Métrica | Valor |
|---|---|
| Filas antes | 55,500 |
| Filas eliminadas | **534** |
| Filas después | **54,966** |
| Duplicados restantes | 0 |

**Justificación:** Los 534 pares identificados en la Fase 1 son filas 100% idénticas en todas sus columnas, por lo que se interpretan como errores de carga o redundancia en la fuente, no como visitas distintas del mismo paciente.

---

## Paso 3 — Estandarización de Formato de Texto

Se normalizaron las columnas con mezcla aleatoria de mayúsculas y minúsculas aplicando `str.title()`:

```python
df_limpio['name']     = df_limpio['name'].str.title()
df_limpio['doctor']   = df_limpio['doctor'].str.title()
df_limpio['hospital'] = df_limpio['hospital'].str.title()
```

**Ejemplo del cambio:**

| Antes | Después |
|---|---|
| `ABIgaIL YOung` | `Abigail Young` |
| `adrIENNE bEll` | `Adrienne Bell` |
| `taNYa DAVIs` | `Tanya Davis` |

---

## Paso 4 — Re-validación de Duplicados Ocultos

Tras normalizar el texto, se realizó una segunda búsqueda de duplicados para detectar registros que antes eran diferentes por el formato de su nombre pero que en realidad eran idénticos.

```python
df_limpio = df_limpio.drop_duplicates(keep='first')
```

| Resultado | Valor |
|---|---|
| Duplicados ocultos encontrados | **0** |
| Total de registros limpios | **54,966** |

> Confirmado: los registros con nombres en formato distinto eran entidades realmente diferentes, no el mismo paciente mal ingresado.

---

## Paso 5 — Conversión de Fechas

Las columnas de fechas, que originalmente estaban como `object` (string), fueron convertidas a `datetime64[ns]`:

```python
df_limpio['date_of_admission'] = pd.to_datetime(df_limpio['date_of_admission'])
df_limpio['discharge_date']    = pd.to_datetime(df_limpio['discharge_date'])
```

**Justificación:** El tipo `datetime64` permite operaciones matemáticas sobre el tiempo (sumas, restas, extracción de componentes como mes o año), lo cual es indispensable para análisis temporales y KPIs de hospitalización.

---

## Paso 6 — Ingeniería de Características: `days_of_stay`

Se creó la variable `days_of_stay` como métrica de negocio derivada de las fechas:

```python
df_limpio['days_of_stay'] = (df_limpio['discharge_date'] - df_limpio['date_of_admission']).dt.days
```

| Métrica | Valor |
|---|---|
| Mínimo | 1 día |
| Máximo | 30 días |
| Valores negativos | **0** — integridad temporal confirmada |

---

## Paso 7 — Tratamiento de `billing_amount` Negativos

En la Fase 1 se detectaron valores negativos en `billing_amount`. Se optó por una **arquitectura de etiquetado categórico** en lugar de simplemente eliminarlos o dejarlos como están.

### Transformaciones aplicadas

```python
# 1. Etiqueta de naturaleza de la transacción
df_limpio['billing_nature'] = np.where(df_limpio['billing_amount'] >= 0, 'Ingreso', 'Ajuste')

# 2. Convertir a valor absoluto
df_limpio['billing_amount'] = df_limpio['billing_amount'].abs()

# 3. Redondear a 2 decimales (estándar contable)
df_limpio['billing_amount'] = df_limpio['billing_amount'].round(2)

# 4. Optimizar tipo de dato
df_limpio['billing_nature'] = df_limpio['billing_nature'].astype('category')
```

### Resultado

| `billing_nature` | Descripción | Frecuencia |
|---|---|---|
| `Ingreso` | Cobro normal al paciente | 54,860 |
| `Ajuste` | Reembolso o crédito contable | 106 |

### Justificación de la decisión

Un signo negativo puede interpretarse erróneamente como un error de ingreso. Al crear `billing_nature`, la naturaleza de la transacción es **explícita y legible** para cualquier consumidor del dato (analistas, dashboards en Power BI/Tableau, modelos de ML). Además, filtrar por `billing_nature == 'Ajuste'` es más eficiente y menos propenso a errores que hacer filtros matemáticos sobre valores menores a cero.

---

## Paso 8 — Validación Final de Integridad

Se ejecutó una verificación completa del dataset limpio antes de exportar:

```python
Nulos encontrados:              0
Valores en blanco:              0
Duplicados:                     0
Errores lógicos en Edad:        0
Errores lógicos en Estancia:    0
Errores lógicos en Montos (Negativos): 0

¡DATASET LIMPIO Y LISTO PARA EXPORTAR!
```

---

## 📐 Esquema Final del Dataset Limpio

| Columna | Tipo | Cambio aplicado |
|---|---|---|
| `name` | object | `snake_case` + `str.title()` |
| `age` | int64 | Sin cambios |
| `gender` | object | `snake_case` |
| `blood_type` | object | `snake_case` |
| `medical_condition` | object | `snake_case` |
| `date_of_admission` | **datetime64[ns]** | Convertida desde `object` |
| `doctor` | object | `snake_case` + `str.title()` |
| `hospital` | object | `snake_case` + `str.title()` |
| `insurance_provider` | object | `snake_case` |
| `billing_amount` | float64 | Valor absoluto + redondeo 2 decimales |
| `room_number` | int64 | `snake_case` |
| `admission_type` | object | `snake_case` |
| `discharge_date` | **datetime64[ns]** | Convertida desde `object` |
| `medication` | object | `snake_case` |
| `test_results` | object | `snake_case` |
| `days_of_stay` | **int64** | Nueva columna |
| `billing_nature` | **category** | Nueva columna |

---

## 🔗 Archivos del Proyecto

| Nombre | Tipo | Enlace / Ubicación | Cloud |
| :--- | :--- | :--- | :--- |
| **Notebook de Trabajo** | 📓 Jupyter Notebook | [`./PruebaTécnica_DataEngineerIntern.ipynb`](./PruebaTécnica_DataEngineerIntern.ipynb) | [Abrir en Google Colab](https://colab.research.google.com/drive/1VKUBwPElwSPr4QK_9rUOPkjW1YL0jMi7?usp=sharing) |
| **Dataset Original** | 🗄️ CSV | [`healthcare_dataset.csv`](./healthcare_dataset.csv) | [Abrir en Kaggle](https://www.kaggle.com/datasets/prasad22/healthcare-dataset) |
| **Dataset Limpio** | ✅ CSV | [`healthcare_dataset_clean.csv`](./healthcare_dataset_clean.csv) | — |