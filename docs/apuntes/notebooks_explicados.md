# Notebooks explicados

## Qué hace cada uno

| Notebook | Pregunta que responde | Resultado clave |
|---|---|---|
| 01 · Mirar los datos | ¿Qué hay en el dataset? | 7 CSV + 120 JSONL; grano de cada tabla; la v2 agrega `carbon_kg` y `genai_tokens` |
| 02 · Perfil de calidad | ¿Cuántos problemas hay? | 12 tipos de problema contados; cada archivo cubre 59 días; 0 huérfanos |
| 03 · Cifras 5V | ¿Con qué números justifico las 5V? | 432 M eventos/día proyectados; GenAI 20 % del costo; 9,5 % SLA incumplido |
| 04 · Watermark | ¿Sirve un watermark con estos datos? | 2 h → 99 % descartado; 60 días → 0 % |
| 05 · MapReduce | ¿Cómo se calcula el mart diario en paralelo? | 11.050 filas; coincide con pandas |

## Trucos de código que usamos

| Código | Qué hace | Ejemplo |
|---|---|---|
| `Path('../data/...')` | Ruta relativa: `..` sube un nivel (de `notebooks/` a la raíz) | `LANDING = Path('../data/datalake/landing')` |
| `glob.glob('*.jsonl')` | Lista archivos que cumplen un patrón | Los 120 archivos de eventos |
| `pd.read_json(a, lines=True)` | Lee un JSONL (un JSON por línea) | Un archivo de eventos |
| `pd.concat([...])` | Pega varias tablas una debajo de otra | Los 120 archivos en una sola tabla |
| `json.loads(linea)` | Convierte texto JSON en diccionario **sin cambiar tipos** | Para ver que `value` llega como texto |
| `df.isna().sum()` | Cuenta los vacíos por columna | Nulos de cada tabla |
| `columna.is_unique` | ¿La clave se repite? | `event_id` es único |
| `(condición).mean()` | **Porcentaje** de filas que cumplen la condición (True = 1, False = 0) | `genai.mean()` → 9,6 % |
| `(condición).sum()` | **Cantidad** de filas que la cumplen | `(costo < 0).sum()` → 216 |
| `quantile(0.99)` | Percentil 99: el valor bajo el cual está el 99 % | Umbral de picos |
| `groupby(col).sum()` | Agrupa y suma, como una tabla dinámica | Costo por servicio |
| `isin([...])` | ¿El valor está en la lista? | Severidad high o critical |
| `cummax()` | Máximo acumulado: "lo más grande que vi hasta acá" | Reloj del watermark |
| `shift(1)` | Corre los valores un lugar | Lo que Spark sabía **antes** de abrir el archivo |
| `.map(serie)` | Le pone a cada fila el valor que corresponde a su clave | El watermark de cada evento según su archivo |
| `pd.Timedelta('2h')` | Una duración que se puede restar a una fecha | Margen del watermark |
| `timestamp[:10]` | Los primeros 10 caracteres: el día | `"2025-08-17T01:55:00Z"` → `"2025-08-17"` |
| `zlib.crc32(...) % 4` | Convierte una clave en un número del 0 al 3, siempre igual para la misma clave | Elegir el reducer |
| `defaultdict(list)` | Diccionario que crea una lista vacía si la clave no existe | Los buzones de cada reducer |

## Entorno (por si algo falla)

```bash
cd "/home/sebastian/Documentos/GitHub/facultad-main/mineria de datos II - miercoles"
source .venv/bin/activate      # siempre antes de jupyter
jupyter lab                    # siempre desde la carpeta principal
```
- `No module named 'pandas'` → Jupyter se abrió sin el entorno. Cerrar, activar y volver a abrir.
- `FileNotFoundError` en `../data/...` → El notebook no está en `notebooks/` o Jupyter se abrió desde otra carpeta.
- **Kernel → Restart Kernel and Run All Cells** corre todo en orden: úsalo antes de entregar.

---

# Entrega 2 · Notebooks del pipeline (PySpark en Colab)

## Qué hace cada uno

| Notebook | Pregunta que responde | Resultado clave |
|---|---|---|
| E2_00 · Setup | ¿Cómo se prepara el entorno? | Copia Landing a Drive una sola vez (inmutable); Spark 4.0.5 |
| E2_01 · Bronze batch | ¿Los maestros llegan completos y sin duplicar? | 80 / 800 / 400 / 240 filas; `cuadra = True`; re-ejecutar no cambia nada |
| E2_02 · Bronze streaming | ¿El stream sobrevive a una falla sin duplicar? | 15 lotes; falla en el lote 5 → `nuevos 0 · ya_existian 3600`; 43.200 eventos únicos |
| E2_03 · Silver | ¿Qué datos se pueden usar y cuáles son sospechosos? | 43.200 en Silver, 0 en quarantine; 2.075 unidades deducidas; 211 negativos y 49 picos marcados |
| E2_04 · Gold | ¿Cuánto gasta cada org por día y servicio? | 11.050 filas · 147.433,98 USD = MapReduce E1; idempotente |
| E2_05 · Pruebas | ¿Funciona ante duplicados, datos tardíos y fallas? | 4 pruebas OK (+1,00 · +12,3456 · +7,00 · 0) |
| E2_06 · Serving | ¿FinOps puede consultar sin Spark? | 2 tablas en AstraDB; carga idempotente; Q1 = Gold |
| E2_07 · Analítica | ¿Qué costos son anómalos? | MAD > 5: 49/49 picos + 33 relativos; `cost_anomaly_mart` 289 filas |
| E2_99 · Evidencia | ¿Qué hay en el lake? | Zip con logs, inventario y muestras |

## Trucos de código de Spark que usamos

| Código | Qué hace | Dónde |
|---|---|---|
| `T.StructType([...])` | Declara el esquema: nombre y tipo de cada columna | `E2_01`, `E2_02` |
| `.option('mode', 'PERMISSIVE')` + `_corrupt_record` | Las filas que no cumplen el esquema no rompen el job: quedan aparte para quarantine | `E2_01` |
| `F.col('_metadata.file_path')` | De qué archivo vino cada fila (linaje) | `E2_01`, `E2_02` |
| `F.lit(valor)` | Una columna con el mismo valor en todas las filas | `ingest_ts`, `batch_id` |
| `.dropDuplicates(['clave'])` | Una fila por clave | maestros, Gold |
| `.write.partitionBy('event_date')` | Una carpeta por valor de la columna | todas las capas |
| `mode('overwrite')` + `partitionOverwriteMode = dynamic` | Reemplaza solo las particiones que se escriben | Silver, Gold |
| `spark.readStream ... .writeStream` | Leer y escribir en modo streaming | `E2_02` |
| `.withWatermark('ingest_ts', '1 hour')` | Hasta cuándo recordar el estado | `E2_02` |
| `.dropDuplicatesWithinWatermark([...])` | Dedupe con estado acotado | `E2_02` |
| `.foreachBatch(funcion)` | Procesar cada micro-lote como un DataFrame normal | `E2_02`, `E2_05` |
| `.join(B, 'event_id', 'left_anti')` | Filas de A que no están en B | barrera 3 |
| `F.expr('try_cast(value as double)')` | Convierte; si no puede, devuelve nulo | `E2_03` |
| `F.create_map(...)[F.col('metric')]` | Un diccionario como columna: metric → unit | `E2_03` |
| `F.broadcast(df)` | Copia una tabla chica a todas las máquinas (sin shuffle) | joins con dimensiones |
| `F.coalesce(a, b)` | El primer valor no nulo | unidad deducida, motivo de quarantine |
| `F.when(cond, valor)` | Un "if" por fila | features, flags |
| `.repartition('event_date')` | Un archivo por día al escribir (compactación) | Gold |
| `F.median`, `F.percentile` | Mediana y percentil exactos | `E2_03`, `E2_07` |
| `.toPandas()` | Pasa un resultado chico a pandas para mostrarlo o graficarlo | `E2_05`, `E2_07` |

## Entorno en Colab (por si algo falla)

- Antes de cada notebook: **Entorno de ejecución → Reiniciar sesión** y después **Ejecutar todas**.
- **Un notebook a la vez:** esperar `✓ sincronizado` antes de abrir el siguiente.
- `JAVA_GATEWAY_EXITED` → la celda de setup no es la estándar (falta `os.environ.pop('JAVA_HOME')` o se instaló PySpark 3.5).
- Datos incompletos o Parquet de 0 bytes → `!rm -rf /content/cpa_datalake`, reiniciar la sesión y volver a correr.
- La tabla completa de errores está en el README, en *Manejo de errores conocidos*.
