# Gobierno preliminar · Entrega 2

Este documento separa lo **implementado** (con evidencia) de lo **propuesto** (política sin automatizar todavía). Los pendientes figuran en el [backlog](backlog_mvp.md).

## 1. Matriz de gobierno por zona

| Zona | Escribe | Lee | Controles implementados | Linaje | Acceso | Retención (propuesta) |
|---|---|---|---|---|---|---|
| **Landing** | `E2_00` (copia única desde el repo) | `E2_01`, `E2_02`, `E2_05` | Inmutable: `E2_00` no copia si ya existe | Archivo original con su nombre | Drive del equipo, solo lectura para los notebooks | 13 meses |
| **Bronze** | `E2_01` (maestros), `E2_02` (eventos) | `E2_03`, `E2_05` | Esquema explícito · `PERMISSIVE` + `_corrupt_record` · dedupe por clave natural · conciliación `landing = bronze + inválidos + duplicados` · anti-join por `event_id` | `source_file`, `ingest_ts`, `ingest_date`, `batch_id` (batch) o `stream_batch_id` (stream) | Equipo de datos | 90 días |
| **Silver** | `E2_03` | `E2_04` | 5 reglas duras → quarantine · 2 flags · conciliación `bronze = silver + quarantine + duplicados` · último snapshot de maestros | Hereda las columnas de Bronze + `unit_deducida` | Equipo de datos y analistas | 13 meses |
| **Gold** | `E2_04`, `E2_07` | `E2_06`, consumo | Assert contra el MapReduce de la entrega 1 · dedupe por `event_id` antes de agregar · recálculo por fecha | `updated_at` por fila; umbrales en `logs/umbrales.json` | Analistas, carga a serving | 24 meses |
| **Quarantine** | `E2_01`, `E2_02`, `E2_03` | Equipo de datos (auditoría) | Cada registro conserva su contenido original + `motivo` + `zona_origen` | `source_file`, `ingest_ts` | Equipo de datos | 90 días |
| **Serving (AstraDB)** | `E2_06` | FinOps (CQL) | Tablas query-first · upsert idempotente · consistencia `LOCAL_QUORUM` | `updated_at` copiado de Gold | Token de aplicación en Colab Secrets | Igual que Gold |
| **Logs** | Todos | Equipo, auditoría | Un registro por corrida o por micro-lote, con conteos | `run_id` / `batch_id` | Equipo | Se versionan en `evidence/e2/logs/` |

## 2. Controles de calidad implementados

| Control | Tipo | Dónde | Resultado en la corrida documentada |
|---|---|---|---|
| Filas ilegibles del CSV | Duro → quarantine `parse_error` | `E2_01` | 0 (antes de la corrección B09 eran 211 en `resources`) |
| Clave natural única en maestros | Dedupe | `E2_01` | 0 duplicados |
| `event_id` no nulo | Duro → `event_id_nulo` | `E2_02`, `E2_03` | 0 en los datos; probado con un caso inyectado |
| `event_id` único | Dedupe en 4 barreras | `E2_02`, `E2_03`, `E2_04` | 43.200 eventos = 43.200 `event_id` distintos |
| `value` numérico | Duro → `value_no_numerico` | `E2_03` | 0 en los datos (1.309 textos numéricos convertidos); probado con `'abc'` |
| `unit` no nula si hay `value` | Duro → `unit_nula_con_value` (después de deducir) | `E2_03` | 0; 2.075 unidades deducidas desde `metric` |
| `schema_version` en {1, 2} | Duro → `schema_version_invalida` | `E2_03` | 0; probado con la versión 9 |
| Integridad referencial org | Duro → `org_desconocida` | `E2_03` | 0; probado con una org inexistente |
| `cost_usd_increment ≥ −0,01` | Blando → `flag_costo_anomalo` | `E2_03` | 211 eventos marcados |
| Pico de costo (> 5 × p99) | Blando → `flag_pico_costo` | `E2_03` | 49 eventos marcados |
| NPS en 0–10 | Blando → `flag_nps_fuera_escala` | `E2_03` | 59 de 80 orgs |
| Pico relativo por serie (MAD > 5) | Analítico → `cost_anomaly_mart` | `E2_07` | 82 filas org × día × servicio |

Las muestras de los casos inyectados están en `quarantine/muestras_reglas` y en `evidence/e2/muestras/`.

## 3. Metadatos: diccionario de las tablas implementadas

| Tabla | Zona | Grano | Partición | Frecuencia | Fuente | Filas | PII |
|---|---|---|---|---|---|---|---|
| `customers_orgs` | Bronze, Silver | 1 fila por org | `ingest_date` (Bronze) | Snapshot diario | `customers_orgs.csv` | 80 | No |
| `users` | Bronze | 1 fila por usuario | `ingest_date` | Snapshot diario | `users.csv` | 800 | **Sí (`email`)** |
| `resources` | Bronze | 1 fila por recurso | `ingest_date` | Snapshot diario | `resources.csv` | 400 | No |
| `billing_monthly` | Bronze | 1 fila por factura | `month` | Mensual | `billing_monthly.csv` | 240 | No |
| `usage_events` | Bronze | 1 fila por evento | `event_date` | Micro-lotes | `usage_events_stream/*.jsonl` | 43.200 | No |
| `usage_events` | Silver | 1 fila por evento válido | `event_date`, `service` | Cierre D+1 | Bronze `usage_events` + orgs + resources | 43.200 | No |
| `org_daily_usage_by_service` | Gold, Serving | org × día × servicio | `event_date` | Cierre D+1 | Silver `usage_events` | 11.050 | No |
| `cost_anomaly_mart` | Gold | org × día × servicio con anomalía | `event_date` | Cierre D+1 | Gold `org_daily_usage_by_service` | 289 | No |
| `cost_anomalies_by_date` | Serving | org × día × servicio con anomalía | `usage_date` (partition key) | Cierre D+1 | Gold `org_daily_usage_by_service` | 256 | No |

El esquema exacto de cada tabla está declarado en el notebook que la escribe (`ESQUEMAS` en `E2_01`, `ESQUEMA_EVENTOS` en `E2_02`) y en [`cql/01_schema.cql`](../../cql/01_schema.cql).

## 4. Linaje de punta a punta

```
landing/usage_events_stream/events_part_XXXX.jsonl
  └─ Bronze usage_events      source_file = ruta del archivo · ingest_ts = llegada · stream_batch_id = micro-lote
      └─ Silver usage_events  mismas columnas + unit_deducida + flags + atributos de org y recurso
          └─ Gold org_daily_usage_by_service   eventos (cuántos se agregaron) · updated_at
              ├─ Gold cost_anomaly_mart        mediana_serie · mad_serie · z_robusto · motivo
              └─ AstraDB finops.*              updated_at copiado de Gold
```

Para rastrear una cifra de Gold hasta su archivo de origen: se filtra Silver por `org_id`, `event_date` y `service`, y la columna `source_file` indica de qué micro-lote vino cada evento.

## 5. Responsabilidades (roles)

| Rol | Responsable de | Capas |
|---|---|---|
| Data engineer | Ingesta, esquemas, checkpoints, quarantine de parse | Landing, Bronze |
| Analytics engineer | Reglas de calidad, conformado, marts, modelo de Cassandra | Silver, Gold, Serving |
| Data steward | Umbrales, decisiones abiertas (D1–D5), revisión de quarantine y de PII | Transversal |
| Consumidor (FinOps) | Consultas Q1 y Q2, validación de las anomalías | Serving |

## 6. Acceso y seguridad

| Aspecto | Implementado | Pendiente |
|---|---|---|
| Credenciales de AstraDB | Token en Colab Secrets; bundle en `MyDrive/cpa_secrets/`; ninguno en el repo ni en los notebooks | Token de solo lectura para consumidores (hoy se usa Database Administrator) |
| Landing | Inmutable (`E2_00` no la pisa) | — |
| PII (`users.email`) | No se propaga más allá de Bronze: no está en Silver, Gold ni Serving | Enmascarar o hashear en Silver `users` antes de usarla |
| Data Lake | Drive personal del responsable de la corrida | Carpeta compartida con permisos por rol |
