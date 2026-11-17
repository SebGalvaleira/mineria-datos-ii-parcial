# Modelo de serving en Cassandra / AstraDB

**Base:** `cloud_provider_analytics` (AstraDB Serverless, non-vector) · **Keyspace:** `finops` · **Scripts:** [`cql/01_schema.cql`](../../cql/01_schema.cql) y [`cql/02_consultas.cql`](../../cql/02_consultas.cql) · **Notebook:** `E2_06_serving_astradb`

## 1. Enfoque query-first

En Cassandra la tabla se diseña a partir de la consulta, no del modelo de datos:
- Solo se puede filtrar por la clave primaria, y en el orden en que fue declarada.
- No hay joins: cada consulta tiene que resolverse leyendo **una partición**.
- Por eso cada pregunta de FinOps tiene su propia tabla. Los atributos que se necesitan (`industry`, `plan_tier`) viajan **desnormalizados** en cada fila.

Las preguntas vienen del §1.2 de la entrega 1: *"¿Cuánto gasta cada org por servicio y por día?"* y *"¿Qué costos son anómalos?"*.

## 2. Matriz consulta-tabla

| | **Q1 · Gasto diario por servicio de una org en un rango de fechas** | **Q2 · Costos anómalos de un día** |
|---|---|---|
| Consumidor | FinOps: seguimiento de una cuenta | FinOps: revisión diaria de alertas |
| Tabla | `org_daily_usage_by_service` | `cost_anomalies_by_date` |
| Origen | Gold `org_daily_usage_by_service` (11.050 filas) | Gold, filas con `eventos_pico > 0` o `eventos_costo_anomalo > 0` (256 filas) |
| **Partition key** | `(org_id, usage_month)` | `usage_date` |
| **Clustering columns** | `usage_date DESC, service ASC` | `org_id, service` |
| Consulta | `WHERE org_id = ? AND usage_month = ? AND usage_date >= ? AND usage_date <= ?` | `WHERE usage_date = ?` |
| Particiones | 160 (80 orgs × 2 meses) | 58 (días con al menos una anomalía, de 60) |
| **Tamaño de partición** | Tope: 31 días × 6 servicios = **186 filas**. Máximo real: **147** | Tope: 80 orgs × 6 servicios = 480 filas. Promedio real ≈ 4, **máximo 11** (23/08) |
| Crecimiento | **Acotado**: cada mes abre particiones nuevas; ninguna crece con el tiempo | Acotado por la cantidad de orgs × servicios |
| Replicación | La administra Astra: réplicas en zonas de disponibilidad de una región. No se declara en el CQL | Igual |
| **Consistencia** | Escritura y lectura en `LOCAL_QUORUM` (mayoría de las réplicas de la región). Astra la exige para escribir; al leer garantiza ver lo último escrito | Igual |
| Carga | Upsert por clave natural (`INSERT` en Cassandra sobrescribe) | Reemplazo de la partición del día: `DELETE` + `INSERT` |
| Resultado verificado | Q1 para `org_53lc58dr`, del 1 al 7/08: 29 filas, **419,1986 USD = Gold** | Q2 del 23/08: 11 filas (5 picos y 6 costos negativos) |

## 3. Justificación de cada decisión

**Q1 · ¿Por qué `(org_id, usage_month)` y no solo `org_id`?**
Con `org_id` sola, la partición de una org crecería para siempre: unas 2.190 filas por año (365 × 6) y sin límite con los años. El bucket mensual la acota a 186 filas como máximo. El costo es que un rango que cruza meses necesita una consulta por mes, que la aplicación junta. Es aceptable porque los reportes de FinOps son mensuales.

**Q1 · ¿Por qué `usage_date` como clustering y `DESC`?**
Dentro de la partición las filas quedan ordenadas en disco por fecha. Un rango de fechas es una lectura secuencial, y lo más reciente sale primero, que es lo que más se consulta. `service` se agrega a la clave para que cada fila sea única: el grano es org × día × servicio.

**Q2 · ¿Por qué `usage_date` como partición?**
La pregunta es "¿qué pasó el día D?". Todas las anomalías del día quedan juntas y se leen de una vez. Ordenar por costo se hace en el cliente: son 11 filas como máximo.

**Q2 · ¿Por qué no poner el costo en la clave para ordenar?**
El costo de un día puede cambiar si llega un evento tardío. Si fuera parte de la clave, el valor nuevo sería **otra fila** y la vieja quedaría huérfana. El upsert dejaría de ser idempotente.

**¿Por qué reemplazar la partición en Q2 en vez de hacer upsert?**
Si al recalcular una anomalía desaparece, un upsert no la borraría. Borrar e insertar el día completo replica el overwrite por partición de Gold. Es un tombstone de partición por día, no uno por celda.

**Nulos:** las métricas que no aplican (por ejemplo, `carbon_kg` en eventos v1) se envían como `UNSET_VALUE` y no como `null`. En Cassandra, escribir `null` genera un tombstone por celda.

## 4. Consultas que el modelo no soporta (a propósito)

```sql
SELECT * FROM finops.org_daily_usage_by_service WHERE service = 'genai';
-- InvalidRequest: PRIMARY KEY column "service" cannot be restricted
-- as preceding column "usage_date" is not restricted
```

Filtrar por una columna que no es la partition key obligaría a recorrer todas las particiones del cluster. Si FinOps necesitara "costo de genai de todas las orgs por día", se crearía otra tabla con `PRIMARY KEY ((service, usage_month), usage_date, org_id)`, cargada desde el mismo mart de Gold. `ALLOW FILTERING` no es una solución: hace el mismo recorrido completo.

## 5. Carga desde Spark

| Aspecto | Implementación |
|---|---|
| Lectura | Gold Parquet → DataFrame con tipos explícitos (`double`, `bigint`, `int`, `date`) |
| Escritura | Driver de Python, `execute_concurrent_with_args` con 64 escrituras en paralelo y sentencias preparadas. En la corrida documentada: 11.050 filas en unos 12 s |
| ¿Por qué no el conector de Spark? | El spark-cassandra-connector no soporta Spark 4. En producción la misma función de upsert se llama desde `foreachBatch` |
| Idempotencia | Se cargó dos veces: 11.050 → 11.050 filas |
| Validación | `COUNT(*)` solo para verificar la carga (Astra avisa que no usa la partition key); suma de Q1 = suma en Gold |
| Credenciales | Token en Colab Secrets (`ASTRA_DB_TOKEN`), bundle en `MyDrive/cpa_secrets/`. Ninguno en el repo |

## 6. Evidencia
- `evidence/e2/capturas/astra_q1.png`, `astra_q2.png`, `astra_rechazo.png`, `astra_schema.png`: CQL Console de Astra.
- `evidence/e2/logs/astra_carga.json`, `astra_q1.csv`, `astra_q2.csv`, `astra_schema.cql`, `astra_consultas.cql`.
