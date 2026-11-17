# Conceptos de la entrega 2

Continúa [`conceptos.md`](conceptos.md) con lo nuevo de la implementación. Cada concepto tiene la idea en una frase, cómo lo usamos y la frase para la defensa.

## Índice
1. [Spark: DataFrame, ejecución perezosa y particiones](#1-spark-dataframe-ejecución-perezosa-y-particiones)
2. [Shuffle y broadcast join](#2-shuffle-y-broadcast-join)
3. [Esquema explícito, PERMISSIVE y `_corrupt_record`](#3-esquema-explícito-permissive-y-_corrupt_record)
4. [`try_cast`](#4-try_cast)
5. [Structured Streaming y micro-lotes](#5-structured-streaming-y-micro-lotes)
6. [Triggers: `availableNow` y `maxFilesPerTrigger`](#6-triggers-availablenow-y-maxfilespertrigger)
7. [Checkpoint en la práctica](#7-checkpoint-en-la-práctica)
8. [`foreachBatch` y at-least-once](#8-foreachbatch-y-at-least-once)
9. [Estado, watermark y `dropDuplicatesWithinWatermark`](#9-estado-watermark-y-dropduplicateswithinwatermark)
10. [Anti-join](#10-anti-join)
11. [Overwrite dinámico de particiones](#11-overwrite-dinámico-de-particiones)
12. [Idempotencia capa por capa](#12-idempotencia-capa-por-capa)
13. [Cassandra: partition key, clustering y tamaño de partición](#13-cassandra-partition-key-clustering-y-tamaño-de-partición)
14. [Cassandra: replicación y consistencia](#14-cassandra-replicación-y-consistencia)
15. [Cassandra: upsert, tombstones y `UNSET_VALUE`](#15-cassandra-upsert-tombstones-y-unset_value)
16. [Mediana, MAD y z robusto](#16-mediana-mad-y-z-robusto)
17. [Consistencia eventual (Google Drive)](#17-consistencia-eventual-google-drive)

---

## 1. Spark: DataFrame, ejecución perezosa y particiones
**Idea:** un DataFrame de Spark es una tabla repartida en **particiones** que se procesan en paralelo. Las transformaciones (`filter`, `withColumn`, `join`) **no se ejecutan** hasta que una acción (`count`, `write`, `show`) pide un resultado.
- Spark arma primero un **plan** con todas las transformaciones y lo optimiza completo. Por ejemplo, empuja los filtros hacia la lectura y lee solo las columnas que se usan.
- `cache()` guarda el resultado en memoria cuando se va a usar varias veces. Lo usamos en `E2_07` con `base` y `scores`.
- **No confundir** las particiones de Spark (pedazos en memoria para paralelizar) con las particiones del Data Lake (carpetas `event_date=...` en disco).

**Para la defensa:** "Spark ejecuta perezosamente: construye el plan completo y lo optimiza antes de tocar los datos."

## 2. Shuffle y broadcast join
**Idea:** el **shuffle** es mover filas entre máquinas para que las que tienen la misma clave queden juntas. Es la operación más cara, igual que el shuffle de MapReduce.
- `groupBy` y `join` normalmente generan shuffle. `spark.sql.shuffle.partitions = 8` fija en cuántos pedazos se reparte; el valor por defecto es 200, demasiado para datos chicos.
- **Broadcast join:** si una tabla es chica (80 orgs, 400 recursos), se copia **entera** a cada máquina y la tabla grande no se mueve. Así se evita el shuffle de los eventos.

**Para la defensa:** "Las dimensiones son chicas, así que las enviamos por broadcast y los 43.200 eventos no viajan por la red."

## 3. Esquema explícito, PERMISSIVE y `_corrupt_record`
**Idea:** declaramos los tipos de cada columna (`StructType`) en lugar de dejar que Spark los adivine.
- `inferSchema` lee una muestra y adivina. Si un archivo trae un número como texto, cambia el tipo de la columna entera.
- Con `mode = PERMISSIVE`, una fila que no cumple el esquema **no rompe el job**: se carga con nulos y el texto original queda en `_corrupt_record`. Esas filas van a quarantine con `motivo = parse_error`.
- Así encontramos el problema de `resources.csv`: 211 filas "corruptas" que en realidad eran válidas, con comillas dobladas dentro de `tags_json`. Se resolvió con `escape='"'`.

**Para la defensa:** "La quarantine no solo atrapa datos malos: también nos mostró un error de lectura nuestro."

## 4. `try_cast`
**Idea:** intenta convertir un valor y, si no puede, devuelve `null` en lugar de fallar.
- `value` llega como número, como texto (`"12.5"`) o nulo. `try_cast(value as double)` convierte los textos numéricos.
- Si `value` tenía algo pero el resultado es `null` (por ejemplo, `'abc'`), es la regla dura `value_no_numerico`.

## 5. Structured Streaming y micro-lotes
**Idea:** Spark trata un flujo de datos como una tabla que **crece**. Cada cierto tiempo toma lo nuevo como un **micro-lote** y lo procesa con el mismo código que un DataFrame normal.
- `spark.readStream.schema(...).json(carpeta)` vigila la carpeta: cada archivo nuevo es dato nuevo.
- En la demo, `llegar(n)` copia archivos de Landing a `stream_input/` para simular que van llegando.

## 6. Triggers: `availableNow` y `maxFilesPerTrigger`
**Idea:** el trigger decide **cuándo** se dispara cada micro-lote.
- `processingTime='1 minute'`: un lote por minuto, para siempre. Es lo que se usaría en producción.
- `availableNow=True`: procesa todo lo que hay en ese momento, en lotes, y **termina**. Hace la demo reproducible.
- `maxFilesPerTrigger = 10`: cada lote toma como máximo 10 archivos. Por eso 40 archivos son 4 lotes con datos.

## 7. Checkpoint en la práctica
**Idea:** una carpeta donde Spark anota qué archivos leyó cada lote (*offsets*) y qué lotes **terminaron** (*commits*).
- Si el job se cae **antes** del commit de un lote, al reiniciar ese lote se **repite completo**, con el mismo `batch_id`.
- Es la barrera 1: impide releer archivos ya confirmados, pero **no** evita el reproceso del lote que se cayó. Por eso existen las otras barreras.

**Para la defensa:** "El checkpoint garantiza que no se pierde nada; las otras barreras garantizan que lo repetido no se duplica."

## 8. `foreachBatch` y at-least-once
**Idea:** `foreachBatch(funcion)` le pasa cada micro-lote a una función nuestra como un DataFrame normal. Ahí se puede hacer cualquier cosa: joins contra tablas estáticas, escribir en varios lugares, recalcular Gold.
- La garantía de `foreachBatch` es ***at-least-once***: cada lote se procesa **al menos** una vez, y puede ser más si hubo una falla.
- Para que el resultado sea "una sola vez" (*exactly-once efectivo*), todo lo que hace la función tiene que ser **idempotente** (ver §12).

## 9. Estado, watermark y `dropDuplicatesWithinWatermark`
**Idea:** para deduplicar un stream, Spark tiene que **recordar** los `event_id` que ya vio. Esa memoria es el **estado**, y si nadie la limpia crece para siempre.
- El **watermark** le dice a Spark hasta cuándo recordar: `withWatermark('ingest_ts', '1 hour')` significa "un dato que llegó hace más de 1 h respecto del último que vi ya no va a tener duplicados".
- `dropDuplicatesWithinWatermark(['dedupe_key'])` deduplica y **borra del estado** cada clave cuando el watermark la pasa.
- **La trampa que marcó el profesor:** `withWatermark` + `dropDuplicates(['event_id'])` **no** limpia el estado, porque la clave no incluye la columna del watermark. El estado crece sin límite.
- Los **lotes sin datos** (4, 9 y 14 en nuestro log) existen para esto: avanzan el watermark y Spark limpia el estado vencido.
- Usamos `ingest_ts` (llegada) y no `timestamp` (evento) porque cada lote trae eventos de 60 días. Con el tiempo de evento, o se descarta casi todo o hay que recordar 60 días.

**Para la defensa:** "El watermark acota la memoria de la deduplicación, no la corrección del resultado. La corrección la garantizan el anti-join y el dedupe de Gold, que no dependen del estado."

## 10. Anti-join
**Idea:** `A.join(B, 'event_id', 'left_anti')` devuelve las filas de A cuya clave **no está** en B.
- En `foreachBatch`: `lote left_anti Bronze` → solo los eventos que todavía no están en Bronze. Es la barrera 3.
- Es lo que hizo que el reproceso del lote 5 diera `nuevos 0 · ya_existian 3600`.

## 11. Overwrite dinámico de particiones
**Idea:** con `partitionOverwriteMode = dynamic`, `mode('overwrite')` reemplaza **solo las particiones que aparecen en los datos que se escriben**. Las demás quedan intactas.
- Con el modo estático (el que viene por defecto), escribir el 03/07 borraría las otras 59 fechas.
- Es lo que permite recalcular un día por un evento tardío sin tocar el resto. La prueba 2 lo verifica con `updated_at`.

## 12. Idempotencia capa por capa
**Idea:** ejecutar dos veces da el mismo resultado que ejecutar una vez.

| Capa | Mecanismo | Prueba |
|---|---|---|
| Bronze batch | Overwrite + dedupe por clave natural | `E2_01`: conteo antes = después |
| Bronze streaming | Checkpoint + anti-join | `E2_02`: reproceso del lote 5 |
| Silver | Recalcula las fechas afectadas con overwrite dinámico | `E2_05` |
| Gold | Dedupe por `event_id` + overwrite dinámico | `E2_04`: recálculo total y parcial sin cambios |
| Cassandra | Upsert por clave primaria; Q2 reemplaza el día | `E2_06`: 11.050 → 11.050 |

## 13. Cassandra: partition key, clustering y tamaño de partición
**Idea:** `PRIMARY KEY ((org_id, usage_month), usage_date, service)`
- Lo que va entre los **paréntesis internos** es la **partition key**: se hashea y decide en qué nodo vive la fila. **Siempre** tiene que estar completa en el `WHERE`.
- Lo que sigue son las **clustering columns**: ordenan las filas dentro de la partición y permiten rangos (`usage_date >= ...`). Solo se pueden filtrar **en orden**: no se puede filtrar por `service` sin antes fijar `usage_date`.
- **Tamaño de partición:** una partición que crece sin límite se vuelve lenta y desbalancea el cluster. El *bucket* `usage_month` la acota a 31 × 6 = 186 filas.
- **Query-first:** primero la consulta, después la tabla. Una consulta distinta lleva otra tabla, con los datos repetidos (desnormalización).

## 14. Cassandra: replicación y consistencia
**Idea:** cada fila se guarda en varias réplicas (*factor de replicación*). La consistencia dice **cuántas** tienen que responder.
- `ONE`: responde una réplica. Es rápido, pero puede devolver un dato viejo.
- `LOCAL_QUORUM`: responde la mayoría de las réplicas de la región (2 de 3). Si se escribe y se lee con quórum, la lectura siempre ve lo último escrito.
- En AstraDB la replicación la administra el servicio; Astra exige quórum para escribir.

## 15. Cassandra: upsert, tombstones y `UNSET_VALUE`
- **Upsert:** en Cassandra `INSERT` y `UPDATE` son lo mismo. Si la clave existe, se sobrescribe. Por eso recargar no duplica.
- **Tombstone:** Cassandra no borra en el momento; escribe una marca de "borrado". Escribir `null` en una columna **también** crea un tombstone. Si hay muchas, las lecturas se vuelven lentas.
- `UNSET_VALUE` (driver de Python): la columna directamente no se envía. La usamos para métricas que no aplican, como `carbon_kg` en eventos v1.

## 16. Mediana, MAD y z robusto
**Idea:** medir qué tan lejos de lo normal está un valor, de una forma que los propios valores extremos no distorsionen.
- **z-score clásico:** (valor − media) / desvío. El problema es que un pico grande infla la media y el desvío, y entonces el pico parece menos raro (**enmascaramiento**).
- **Mediana:** el valor del medio. Un pico no la mueve.
- **MAD** (*median absolute deviation*): la mediana de las distancias a la mediana. Es el "desvío" robusto.
- **z robusto** = 0,6745 × (valor − mediana) / MAD. El 0,6745 lo pone en la misma escala que un z-score normal.
- Se calcula **por serie** (cada org con cada servicio), así cada serie se compara contra sí misma.

**Para la defensa:** "Un umbral único en USD es ciego a los servicios baratos. El z robusto compara cada serie con su propia mediana: 27 USD de networking es 18 veces lo normal para esa org."

## 17. Consistencia eventual (Google Drive)
**Idea:** en algunos sistemas de archivos remotos, un cambio no se ve igual en todos lados **inmediatamente**: se vuelve consistente "con el tiempo".
- En Drive, un archivo puede aparecer listado **antes** de que se termine de subir su contenido. Spark leyó Parquet de 0 bytes.
- **Solución:** trabajar en el disco local de Colab, que es consistente, y copiar a Drive al final con `rsync`. Y un notebook a la vez.
- Es el mismo problema por el que en la nube real (S3) se usan formatos con *commit* atómico, como Delta Lake o Iceberg.
