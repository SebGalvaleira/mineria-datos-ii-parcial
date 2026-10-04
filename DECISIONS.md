# Registro de decisiones

| ID | Fecha | Decisión | Alternativas | Justificación | Estado |
|---|---|---|---|---|---|
| D01 | 2026-10-03 | Documento de diseño en Markdown dentro del repo | PDF | La consigna acepta ambos; Markdown se versiona y se lee en GitHub | Aceptada |
| D02 | 2026-10-03 | El volumen se justifica con una proyección de escala real (supuesto), no con la muestra | Justificar solo con la muestra | La muestra es de ~13 MB; el diseño debe escalar sin rediseño | Aceptada |
| D03 | 2026-10-03 | Exploración inicial con pandas; el pipeline se implementa en PySpark | Explorar directamente en Spark | La muestra entra en memoria; los mismos chequeos se portan a Spark en la entrega 2 | Aceptada |
| D04 | 2026-10-03 | Las anomalías (costos negativos, spikes) se marcan con flags y no se borran | Descartarlas | Pueden ser créditos o picos reales; borrarlas sesga el costo y quita valor a FinOps | Aceptada |
| D05 | 2026-10-03 | `unit` nula se infiere desde `metric` | Dejar nula / descartar | La relación metric→unit es 1 a 1 en el 100 % de los registros con unidad | Aceptada |
| D06 | — | Tratamiento de subtotales en ARS y tipo de cambio en USD ≠ 1 | Subtotal ya en USD / aplicar tc | Pendiente de confirmar con la cátedra | Abierta |
| D07 | — | Escala de NPS (valores −38 a 101) | Reescalar / descartar / tratar como NPS de cuenta | Pendiente de confirmar con la cátedra | Abierta |
| D08 | 2026-10-04 | No usar ventanas por tiempo de evento en el stream; `foreachBatch` recalcula las claves (org, fecha, servicio) afectadas y hace upsert | Ventana con watermark corto | La simulación muestra que un watermark de 2 h descartaría el 99 % de los eventos (notebook 02) | Aceptada |
| D09 | 2026-10-04 | Watermark solo para acotar el estado de la deduplicación por `event_id`, sobre `ingest_ts` | Watermark sobre `timestamp` del evento | Los reintentos llegan cerca en el tiempo de llegada, no de evento | Aceptada |
| D10 | 2026-10-04 | Cierre batch D+1 que recalcula el día anterior y concilia contra billing | Confiar solo en el camino rápido | Control de consistencia y cobertura de O2 y O8 | Aceptada |
| D11 | 2026-10-04 | Diagrama en Graphviz (`.dot`) exportado a PNG | Mermaid | Mermaid no controla bien el layout de los dos caminos; el PNG se ve igual en GitHub y en PDF | Aceptada |
