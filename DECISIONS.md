# Registro de decisiones

| ID | Decisión | Alternativas | Justificación | Evidencia | Estado |
|---|---|---|---|---|---|
| A01 | Patrón **híbrido**: streaming para eventos, batch para maestros y billing, cierre D+1 | Batch puro · Lambda · Kappa | Los datos tienen dos ritmos; la conciliación con billing es mensual; se reutiliza la misma lógica en ambos caminos | §5 | Aceptada |
| A02 | No agregar por ventanas dentro del stream; recalcular las claves afectadas por lote y hacer *upsert* | Ventanas con watermark | Un watermark de 2 h descartaría el 99 % de los eventos | notebook 04 | Aceptada |
| A03 | Watermark solo para acotar la deduplicación por `event_id` | Watermark sobre el tiempo de evento | Los reintentos llegan cerca en el tiempo de llegada | §4.3 | Aceptada |
| A04 | Particionar eventos por **fecha del evento** | Fecha de llegada | Cada micro-lote mezcla los 60 días | notebook 02 | Aceptada |
| A05 | Parquet desde Bronze | CSV / JSON | Columnar, comprimido, con tipos; permite leer solo las columnas necesarias | §7 | Aceptada |
| A06 | Anomalías (negativos, picos) se **marcan**, no se borran | Descartarlas | Pueden ser reales; los picos son el 6,3 % del costo | notebook 02 y 03 | Aceptada |
| A07 | `unit` nula se deduce desde `metric` | Dejar nula / descartar | La relación metric → unit es 1 a 1 en el 100 % de los casos con unidad | notebook 02 | Aceptada |
| A08 | Quarantine con motivo para registros inválidos | Descartarlos | Permite auditar y conciliar Landing = Silver + Quarantine | §3.4 | Aceptada |
| A09 | Volumen justificado con proyección de escala real (supuesto) | Justificar con la muestra | La muestra es de 12,9 MB | notebook 03 | Aceptada |
| A10 | Exploración en pandas; pipeline en PySpark (entrega 2) | Explorar en Spark | La muestra entra en memoria; Spark es opcional en esta etapa | — | Aceptada |
| D1 | ¿Subtotal en ARS ya está en USD? | Ya en USD · tc erróneo | Pendiente de la cátedra | §10.3 | Abierta |
| D2 | Escala del NPS (−38 a 101) | NPS de cuenta · descartar | Pendiente de la cátedra | §10.3 | Abierta |
| D3 | Umbral de picos | 5 × p99 · z-score · MAD | Se evalúa en la entrega 2 | §10.3 | Abierta |
| D4 | Retención definitiva | §7.4 u otra | Pendiente del negocio | §7.4 | Abierta |
| D5 | CSAT > 5 | Quarantine · recortar | Propuesta: quarantine | §10.3 | Abierta |
