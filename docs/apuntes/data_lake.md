# Apunte · Data Lake del proyecto Cloud Provider Analytics

## 1. Qué es un Data Lake

Un **lugar central donde se guardan todos los datos**, crudos y procesados, organizados por **zonas** según qué tan limpios están. A diferencia de una base de datos tradicional, acepta cualquier formato (CSV, JSON, Parquet) y no obliga a definir la estructura antes de guardar.

**Idea clave:** el dato avanza de zona en zona y en cada paso queda más limpio y más útil. **Nunca se modifica el original.**

---

## 2. Las zonas (con analogía de cocina)

| Zona | Analogía | Qué contiene | Regla de oro |
|---|---|---|---|
| **Landing** | La mercadería tal como llega del proveedor | Los archivos originales (CSV, JSONL) | **Inmutable:** no se toca, no se borra, no se agrega nada |
| **Bronze** | La mercadería guardada en la heladera, etiquetada | Los mismos datos, **mismo grano**, con tipos definidos y columnas técnicas (`source_file`, `ingest_ts`) | Fiel a la fuente: no se limpia, solo se ordena |
| **Silver** | Los ingredientes lavados y cortados | Datos **limpios y conformados**: tipos convertidos, `unit` deducida, flags de anomalías, v1/v2 unificadas, monedas en USD, joins con dimensiones | Acá viven las reglas de calidad |
| **Gold** | El plato servido | **Marts**: tablas resumidas para cada usuario (FinOps, Soporte, Producto) | Grano fijo y pensado para consultas |
| **Quarantine** | El tacho de "esto está podrido", con etiqueta | Registros inválidos **con el motivo** del rechazo | Nada se borra en silencio |

---

## 3. Quién escribe, quién lee y cuándo se promueve

| Zona | Escribe | Lee | Se promueve a la siguiente cuando… |
|---|---|---|---|
| Landing | El sistema de origen (llegan los archivos) | Jobs de ingesta | El archivo se puede leer y su esquema coincide con lo esperado |
| Bronze | Jobs de ingesta (batch y streaming) | Jobs de conformado | Los tipos son válidos y pasa las reglas mínimas de calidad |
| Silver | Jobs de conformado | Jobs de agregación, analistas | Los joins están validados y las métricas son consistentes |
| Gold | Jobs de agregación | Cassandra/AstraDB, dashboards | El mart está estable y su grano está documentado |
| Quarantine | Cualquier job que rechaza un registro | Equipo de datos (auditoría) | Se corrige y se reprocesa, o se descarta con justificación |

---

## 4. Formato: por qué Parquet

| | CSV / JSON | **Parquet** |
|---|---|---|
| Cómo guarda | Fila por fila, como texto | **Columna por columna**, en binario |
| Tamaño | Grande | Comprimido (suele ocupar 5 a 10 veces menos) |
| Leer una sola columna | Hay que leer todo el archivo | Lee **solo esa columna** |
| Tipos | Todo es texto | Cada columna tiene su tipo (número, fecha…) |

**Por qué importa:** FinOps casi siempre consulta pocas columnas (`org_id`, `fecha`, `costo`). Con Parquet se lee solo eso.

---

## 5. Particiones

**Particionar** = dividir los datos en carpetas según el valor de una columna:

```
silver/usage_events/event_date=2025-07-15/part-0001.parquet
silver/usage_events/event_date=2025-07-16/part-0001.parquet
```

Una consulta del tipo "dame el costo del 15/07" **solo abre la carpeta de ese día** (esto se llama *partition pruning*).

**Reglas:**
- Particionar por columnas que se usan para **filtrar** (la fecha casi siempre).
- **Nunca** por columnas con muchísimos valores distintos (como `event_id`): saldrían millones de carpetas con archivos diminutos.
- En este caso, por **fecha del evento** (`event_date`) y **no** por fecha de llegada, porque los eventos llegan desordenados.

---

## 6. Linaje y trazabilidad

Cada fila de Bronze lleva:
- `source_file`: de qué archivo de Landing vino.
- `ingest_ts`: cuándo se ingirió.

Así, ante un número raro en Gold, se puede rastrear: **Gold → Silver → Bronze → el archivo exacto en Landing**.

**Conciliación (objetivo O3):**
```
filas en Landing = filas en Bronze = filas en Silver + filas en Quarantine
```
Si la cuenta no da, se perdió algo en el camino.

---

## 7. Los 5 tratamientos de calidad (en Silver)

| Tratamiento | Cuándo | Ejemplo del caso |
|---|---|---|
| **Convertir** | Dato correcto, tipo equivocado | `value` = `"8.94"` (texto) → 8,94 |
| **Deducir** | Falta, pero se calcula con certeza | `unit` vacía → sale de `metric` |
| **Marcar (flag)** | Sospechoso, pero puede ser real | Costo negativo → `flag_negative_cost = true` |
| **Quarantine** | Inválido, no se puede usar | CSAT = 7 en escala 1–5 |
| **Aceptar y documentar** | Raro, pero con explicación | Ticket resuelto después del 31/08 |

---

## 8. Streaming y watermark (la decisión clave del diseño)

**Watermark** = hora del evento más nuevo visto − margen tolerado. Lo que sea más viejo se descarta.

**Hallazgo (notebook 04):** como cada micro-lote trae eventos de los 60 días, un watermark descartaría:

| Margen | 2 h | 1 día | 7 días | 30 días | 60 días |
|---|---|---|---|---|---|
| % descartado | 99 % | 97,5 % | 87,6 % | 49,6 % | 0 % |

**Solución adoptada:**
- **A. Guardar todo:** el stream escribe Bronze y Silver en modo *append*, sin descartar eventos tardíos.
- **B. Recalcular lo que cambió:** por cada lote se recalculan desde Silver las claves (org, fecha, servicio) que aparecieron y se hace *upsert* (reemplazo) en Gold.
- **C. Control D+1:** cada noche, un proceso batch recalcula el día anterior completo.

El watermark se usa **solo** para limitar la memoria de la deduplicación por `event_id`.

---

## 9. Glosario rápido

| Término | Significado |
|---|---|
| **Grano** | Qué representa una fila (un evento, una factura de org × mes…) |
| **Batch** | Procesar un bloque de datos de una vez, cada cierto tiempo (diario, mensual) |
| **Streaming** | Procesar los datos a medida que llegan, en lotes chicos (cada minuto) |
| **Micro-lote** | Un archivo chico de eventos que llega al stream (acá, 360 eventos) |
| **Append** | Solo agregar filas nuevas, nunca modificar las existentes |
| **Upsert** | Si la clave existe, reemplazar; si no, insertar |
| **Idempotencia** | Procesar dos veces lo mismo da el mismo resultado (no duplica) |
| **Checkpoint** | Registro de qué archivos ya procesó el stream, para no repetirlos si se reinicia |
| **Join** | Cruzar dos tablas por una columna en común (por ejemplo, eventos + organizaciones por `org_id`) |
| **Huérfano** | Fila que apunta a algo que no existe (un ticket de una org inexistente) |
| **Mart** | Tabla final orientada a un usuario y a sus preguntas |
| **PII** | Dato personal (email, nombre): se enmascara antes de Gold |
| **Partition pruning** | Leer solo las carpetas de partición necesarias para una consulta |
| **D+1** | El día de ayer está cerrado y disponible hoy temprano |