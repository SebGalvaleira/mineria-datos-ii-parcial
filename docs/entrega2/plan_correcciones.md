# Plan de correcciones · Entrega 2

## 1. Feedback recibido en la entrega 1


Puntos concretos:
1. **Tres pruebas controladas:** un evento duplicado, un evento extremadamente tardío y un micro-lote reprocesado después de un fallo.
2. **Advertencia técnica:** no hay que asumir que cualquier combinación de `withWatermark` y `dropDuplicates` acota el estado como se espera.
3. **Defensa:** los cuatro integrantes tienen que poder explicar las decisiones.

## 2. Plan y estado

| ID | Corrección | Prioridad | Fecha objetivo | Evidencia esperada | Estado |
|---|---|---|---|---|---|
| C01 | **Prueba: evento duplicado.** El mismo `event_id` llega varias veces y se cuenta una sola vez en Bronze, Silver y Gold | Alta | 16/11 | `E2_05` prueba 1: Gold cambia en 1 fila (+1 evento, +1,00 USD) |  Hecho |
| C02 | **Prueba: evento extremadamente tardío.** Un evento del 03/07 llega último (59 días tarde), no se pierde y corrige solo su día | Alta | 16/11 | `E2_05` prueba 2: solo se recalcula el 03/07 (+12,3456 USD exactos); `updated_at` de los otros 59 días sin cambios |  Hecho |
| C03 | **Prueba: micro-lote reprocesado.** El job cae después de escribir en Bronze y se reinicia con el mismo checkpoint | Alta | 16/11 | `E2_02`: lote 5 reprocesado con `nuevos 0 · ya_existian 3600`. `E2_05` prueba 3: Gold suma +7,00 USD una sola vez |  Hecho |
| C04 | **Estado acotado del dedupe (advertencia del feedback).** Watermark sobre `ingest_ts` + `dropDuplicatesWithinWatermark` para los reintentos cercanos. Las garantías que **no dependen del estado** son el anti-join contra Bronze y el dedupe por `event_id` en Gold | Alta | 16/11 | `E2_02` (lotes sin datos que avanzan el watermark); `E2_05` prueba 4: un duplicado inyectado en Silver no cambia Gold |  Hecho |
| C05 | **Diagrama v2 consistente con lo implementado.** Lo pendiente se muestra en gris como backlog | Media | 17/11 | `docs/img/arquitectura_v2.png` |  Hecho |
| C06 | **Decisión abierta D3 (umbral de picos):** evaluar z-score, percentil y MAD con datos | Media | 17/11 | `E2_07`: tabla de comparación y sensibilidad |  Hecho |
| C07 | **Defensa por los cuatro integrantes.** Cada uno explica al menos una capa y una prueba | Alta |  18/11 | `docs/apuntes/guia_defensa.md`, ensayo previo | ⏳ En curso |

## 3. Correcciones que surgieron al implementar

Problemas que no estaban en el feedback y que aparecieron al ejecutar el pipeline. Cada uno quedó corregido y registrado en `DECISIONS.md`.

| ID | Problema encontrado | Cómo se detectó | Corrección | Decisión |
|---|---|---|---|---|
| C08 | 211 filas válidas de `resources.csv` iban a quarantine | La quarantine mostró `parse_error` en filas con `tags_json` | Leer CSV con `escape='"'`: el archivo usa comillas dobladas | B09 |
| C09 | Parquet de 0 bytes al leer Bronze desde el stream | Error `is not a Parquet file` en la segunda tanda | Trabajar en disco local y sincronizar a Drive | B02 |
| C10 | Gold con 12.240 eventos y 17 días en lugar de 43.200 y 60 | El assert contra el MapReduce de la entrega 1 | Un notebook a la vez; esperar la sincronización. Quedó como regla del README | B02 |
| C11 | La primera versión de `guardar_en_drive` con `--delete` podía borrar zonas que escribió otro notebook | Revisión de la tabla de restauración | Cada notebook sincroniza **solo las carpetas que escribe** | B02 |
| C12 | Un micro-lote reprocesado con `nuevos = 0` nunca habría actualizado Silver ni Gold | Al diseñar la prueba 3 | Las fechas afectadas salen de todos los eventos válidos del lote | B06 |
| C13 | Si la ingesta batch corre dos días, el join duplicaría eventos | Revisión de Silver | Usar el último snapshot de cada maestro | B10 |
| C14 | 59 de 80 orgs tienen NPS fuera de 0–10 | Conformado de `customers_orgs` | Conservar el original, agregar `nps_0_10` y un flag | B15 |

## 4. Desvíos respecto del diseño de la entrega 1

El diseño no cambió en lo esencial. Estos son los puntos en que la implementación difiere y por qué:

| Diseño (entrega 1) | Implementación (entrega 2) | Motivo |
|---|---|---|
| Spark 3.5 con spark-cassandra-connector | Spark 4.0.5 y carga con el driver de Python | Colab usa Python 3.13 (B01); el conector no soporta Spark 4 (B13) |
| Streaming con `trigger` de 1 minuto | `trigger(availableNow)` con llegada simulada en 3 tandas de 40 archivos | Hace la demo reproducible y permite inyectar la falla en un lote concreto. En producción solo cambia el `trigger` |
| Orquestación con Airflow | Notebooks ejecutados en orden, con una regla documentada | Alcance del MVP; queda en el backlog como deseable |
| `foreachBatch` recalcula Silver y Gold en el pipeline principal | El pipeline principal corre Silver y Gold como cierre batch; la cadena por micro-lote se demuestra en el sandbox de `E2_05` | Separar las capas facilita explicar y validar cada una. La lógica es la misma |
| 5 marts en Gold | 2 marts: `org_daily_usage_by_service` y `cost_anomaly_mart` | La consigna exige el mart FinOps; el resto queda en el backlog |
| Silver con FX a USD y PII enmascarada | Silver de eventos y de orgs; billing y users quedan en Bronze | Billing depende de D1; `users.email` no se propaga más allá de Bronze. Ambos en el backlog |
| Compactación diaria de todas las particiones | Compactación en Gold (`repartition('event_date')`, 1 archivo por día) | Bronze y Silver quedan con archivos chicos; compactar Silver está en el backlog |
| Umbral de picos 5 × p99 (D3 abierta) | 5 × p99 en Silver y MAD > 5 por serie en Gold | Evaluado con datos en `E2_07` (B14) |
