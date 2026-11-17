# Registro de decisiones

Las decisiones `A..` son de la entrega 1 (diseño). Las `B..` son de la entrega 2 y salen de la implementación: cada una tiene evidencia ejecutable. `D..` son decisiones abiertas.

## Entrega 1 · Diseño

| ID | Decisión | Alternativas | Justificación | Evidencia | Estado |
|---|---|---|---|---|---|
| A01 | Patrón **híbrido**: streaming para eventos, batch para maestros y billing, cierre D+1 | Batch puro · Lambda · Kappa | Los datos tienen dos ritmos; la conciliación con billing es mensual; se reutiliza la misma lógica en ambos caminos | §5 | Aceptada · implementada (ver B04) |
| A02 | No agregar por ventanas dentro del stream; recalcular las claves afectadas por lote y hacer *upsert* | Ventanas con watermark | Un watermark de 2 h descartaría el 99 % de los eventos | notebook 04 | Aceptada · implementada (B07) |
| A03 | Watermark solo para acotar la deduplicación por `event_id` | Watermark sobre el tiempo de evento | Los reintentos llegan cerca en el tiempo de llegada | §4.3 | Aceptada · implementada (B05) |
| A04 | Particionar eventos por **fecha del evento** | Fecha de llegada | Cada micro-lote mezcla los 60 días | notebook 02 | Aceptada · implementada (B03) |
| A05 | Parquet desde Bronze | CSV / JSON | Columnar, comprimido, con tipos; permite leer solo las columnas necesarias | §7 | Aceptada · implementada |
| A06 | Anomalías (negativos, picos) se **marcan**, no se borran | Descartarlas | Pueden ser reales; los picos son el 6,3 % del costo | notebook 02 y 03 | Aceptada · implementada (B08) |
| A07 | `unit` nula se deduce desde `metric` | Dejar nula / descartar | La relación metric → unit es 1 a 1 en el 100 % de los casos con unidad | notebook 02 | Aceptada · implementada: 2.075 unidades deducidas |
| A08 | Quarantine con motivo para registros inválidos | Descartarlos | Permite auditar y conciliar Landing = Silver + Quarantine | §3.4 | Aceptada · implementada (B08) |
| A09 | Volumen justificado con proyección de escala real (supuesto) | Justificar con la muestra | La muestra es de 12,9 MB | notebook 03 | Aceptada |
| A10 | Exploración en pandas; pipeline en PySpark (entrega 2) | Explorar en Spark | La muestra entra en memoria; Spark es opcional en esta etapa | — | Aceptada · cumplida |

## Entrega 2 · Implementación

| ID | Decisión | Alternativas | Justificación | Evidencia |
|---|---|---|---|---|
| B01 | **Google Colab + PySpark 4.0.5** (Python 3.13, Java 21) | Spark 3.5 · instalación local | Colab trae Python 3.13 y Java 21; Spark 3.5 no soporta Python 3.13 (`JAVA_GATEWAY_EXITED`). Spark 4.0 los soporta y tiene `dropDuplicatesWithinWatermark` | `E2_00` |
| B02 | **Trabajar en el disco local de Colab y sincronizar a Drive con `rsync` por zona** | Escribir directo en Drive | Drive tiene consistencia eventual: el stream leyó Parquet de 0 bytes. En local las escrituras son atómicas. Cada notebook restaura solo lo que lee y sincroniza solo lo que escribe, así el `--delete` nunca borra zonas de otro notebook | `E2_02`; README · reglas de ejecución |
| B03 | **Particiones implementadas:** maestros por `ingest_date` (snapshot) · billing por `month` · Bronze eventos por `event_date` · Silver eventos por `event_date` + `service` · Gold por `event_date` con `repartition` (1 archivo por día) | Partir por `org_id` · por fecha de llegada | Las consultas filtran por fecha y servicio (6 valores, baja cardinalidad). `org_id` generaría miles de carpetas chicas. La fecha de llegada no sirve porque cada lote mezcla 60 días | `evidence/e2/inventario_lake.csv` |
| B04 | **Híbrido en la implementación:** streaming de eventos a Bronze (`E2_02`) y batch para maestros (`E2_01`), Silver (`E2_03`) y Gold (`E2_04`) como cierre D+1. La cadena completa por micro-lote (stream → Silver → Gold) se demuestra en el sandbox de `E2_05` | Lambda (dos códigos) · Kappa (todo stream) | Un solo código de transformación sirve a los dos caminos. Kappa obligaría a pasar maestros y facturación mensual por el stream sin beneficio | `E2_02`, `E2_05` |
| B05 | **Deduplicación en capas:** ① checkpoint (no relee archivos) · ② `withWatermark('ingest_ts', '1 hour')` + `dropDuplicatesWithinWatermark` (reintentos cercanos, **estado acotado**) · ③ anti-join por `event_id` contra Bronze dentro de `foreachBatch` · ④ `dropDuplicates(['event_id'])` en Gold antes de agregar | Solo `dropDuplicates` en el stream | Responde a la advertencia del feedback: el watermark sobre la llegada acota el estado, pero un duplicado que llega más tarde se le escapa. La garantía final no depende del estado del stream | `E2_02`; `E2_05` pruebas 1 y 4 |
| B06 | **Las fechas a recalcular salen de todos los eventos válidos del lote**, no solo de los nuevos, y se registran solo si el lote termina bien | Usar solo los eventos nuevos | Encontrado al diseñar la prueba 3: al reprocesar un lote caído, sus eventos ya están en Bronze (`nuevos = 0`) y Silver/Gold nunca se habrían actualizado | `E2_05` prueba 3 |
| B07 | **Gold recalcula fechas completas desde Silver** y sobrescribe solo esas particiones (`partitionOverwriteMode = dynamic`) | Sumar incrementos al mart | Un evento tardío (59 días) corrige su día sin tocar los otros 59. Re-ejecutar no duplica | `E2_04`; `E2_05` prueba 2 |
| B08 | **Reglas duras → quarantine con `motivo`:** `event_id_nulo`, `value_no_numerico`, `unit_nula_con_value`, `schema_version_invalida`, `org_desconocida`. **Reglas blandas → flags:** `flag_costo_anomalo` (costo < −0,01), `flag_pico_costo` (> 5 × p99) | Descartar o corregir en silencio | Lo inválido no se puede usar; lo sospechoso puede ser real. Umbral de costo mínimo −0,01 según la consigna (tolera redondeos) | `E2_03`; `evidence/e2/muestras/quarantine_*` |
| B09 | Leer CSV con `escape='"'` | Comillas por defecto de Spark | `resources.csv` usa comillas dobladas en `tags_json`: sin la opción, 211 filas válidas iban a quarantine. La quarantine detectó un error de lectura, no de datos | `E2_01` |
| B10 | Silver usa el **último snapshot** de cada maestro | Leer todas las particiones `ingest_date` | Si la ingesta corre dos días, habría dos fotos de cada org y el join duplicaría eventos | `E2_03` |
| B11 | **Cassandra Q1:** `PRIMARY KEY ((org_id, usage_month), usage_date, service)` con `usage_date DESC` | `((org_id), usage_date, service)` | El bucket mensual acota la partición (máx. 186 filas; real 147) aunque pasen años | `docs/entrega2/modelo_cassandra.md` |
| B12 | **Cassandra Q2:** `PRIMARY KEY ((usage_date), org_id, service)`; se **reemplaza la partición del día** (DELETE + INSERT) | Costo como clustering | Si el costo fuera clave, al recalcular quedarían filas huérfanas. El reemplazo por día replica el overwrite de Gold | `E2_06` |
| B13 | **Carga a Astra con el driver de Python** (`execute_concurrent`, 64 en paralelo), `LOCAL_QUORUM`, nulos como `UNSET_VALUE` | spark-cassandra-connector | El conector no soporta Spark 4. `INSERT` en Cassandra es upsert, así que recargar no duplica. Escribir `null` crea tombstones | `E2_06`: 11.050 → 11.050 |
| B14 | **Anomalías: z robusto (MAD) > 5 por serie org × servicio**, con mínimo 14 días. Se mantiene la regla 5 × p99 en Silver | z-score clásico · p99 por servicio · ML supervisado | Encuentra 49/49 picos de la regla y 33 picos relativos (28 en storage y networking) que un umbral único en USD no ve. Sensibilidad: con 3,5 da 75 nuevas, con 10 pierde un pico. Sin etiquetas, un modelo supervisado no se justifica | `E2_07`; `logs/anomalias_comparacion_metodos.csv` |
| B15 | **NPS:** se conserva el original, se agrega `nps_0_10` (solo si está en 0–10) y `flag_nps_fuera_escala` | Descartar · recortar | 59 de 80 orgs están fuera de 0–10. No se pierde el dato mientras D2 siga abierta | `E2_03` |
| B16 | **Secretos fuera del repo:** token en Colab Secrets; bundle en `MyDrive/cpa_secrets/` | Token en el código | El token da acceso de administrador a la base | `E2_06` |

## Decisiones abiertas

| ID | Pregunta | Estado en la entrega 2 |
|---|---|---|
| D1 | ¿El subtotal en ARS ya está en USD? | **Abierta.** Billing se ingirió a Bronze tal cual; la conversión y la conciliación billing vs uso quedan en el backlog |
| D2 | Escala del NPS (−38 a 101) | **Provisoria (B15).** Se marca y se conserva; falta confirmar con la cátedra |
| D3 | Umbral de picos | **Cerrada → B08 + B14.** 5 × p99 por evento en Silver y MAD > 5 por serie en Gold |
| D4 | Retención definitiva | **Abierta.** Se mantiene la propuesta de la entrega 1 (§7.4) |
| D5 | CSAT > 5 | **Abierta.** `support_tickets` no se procesa en esta entrega (backlog) |
