# Backlog para el MVP final

Lo que falta priorizado con **MoSCoW**, usando las categorías que pide la consigna: obligatorio (*Must*), deseable (*Should / Could*) y fuera de alcance (*Won't*). Cada ítem tiene un criterio de terminado verificable.

## Obligatorio (Must)

| ID | Ítem | Por qué | Criterio de terminado |
|---|---|---|---|
| M1 | Ingestar `support_tickets`, `nps_surveys` y `marketing_touches` a Bronze | La consigna pide marts para Soporte y Producto; hoy solo hay fuentes de FinOps | 3 tablas en Bronze con `cuadra = True`, usando la misma función de `E2_01` |
| M2 | Mart de Soporte `tickets_by_org_date` (tickets, % de SLA incumplido, CSAT promedio) | Objetivo O7 de la entrega 1 | Mart en Gold y tabla query-first en Astra con una consulta |
| M3 | Mart de Producto `genai_tokens_by_org_date` | Pregunta de Producto sobre adopción de GenAI | Mart en Gold con tokens y costo por org y día |
| M4 | Cargar `cost_anomaly_mart` en Astra con `z_robusto` y `motivo` | Hoy Q2 sirve solo las anomalías por regla; los 33 picos relativos no llegan a FinOps | Q2 devuelve también las filas con `pico_relativo_serie` |
| M5 | Enmascarar `users.email` en Silver | PII; requisito de gobierno de la entrega 1 | Silver `users` con email hasheado; test que verifica que no hay emails en claro |
| M6 | Resolver D1 (moneda de billing) y conciliar billing contra uso | Objetivo O8 de la entrega 1 | Diferencia por org y mes reportada para el 100 % de las orgs |
| M7 | Integrar la cadena por micro-lote (stream → Silver → Gold → Astra) en el pipeline principal | Hoy corre completa solo en el sandbox de `E2_05` | `E2_02` actualiza Gold y Astra en el mismo `foreachBatch` |
| M8 | Mover las funciones repetidas a un módulo `src/` | `construir_silver`, `recalcular_gold` y el setup están copiados en varios notebooks: un cambio hay que hacerlo en todos | Los notebooks importan desde `src/`; ninguna función está duplicada |
| M9 | Confirmar D2 (escala NPS) y D5 (CSAT > 5) con la cátedra | Afectan los marts de Soporte y Producto | Decisiones cerradas en `DECISIONS.md` |

## Deseable (Should / Could)

| ID | Ítem | Por qué | Criterio de terminado |
|---|---|---|---|
| S1 | MAD con **ventana móvil** de 28 días hacia atrás | La mediana actual usa los 60 días, incluido el futuro: sirve para auditoría D+1, no para alertas en vivo | `z_robusto` calculado solo con días anteriores; comparación con la versión actual |
| S2 | Compactar Silver (`repartition('event_date', 'service')`) | Bronze y Silver tienen muchos archivos chicos; restaurar desde Drive tarda varios minutos | 1 archivo por partición en Silver; tiempo de restauración medido antes y después |
| S3 | Notebook `run_all` o un job programado que ejecute 01→07 en orden | Hoy la regla "un notebook a la vez" depende de la persona | Un solo comando ejecuta el pipeline y falla si algún assert falla |
| S4 | Pruebas automáticas de las reglas (`pytest` sobre `src/`) | Hoy las pruebas viven dentro de los notebooks | Los casos de `E2_03` y `E2_05` corren como tests |
| S5 | Token de solo lectura para consumidores | Hoy se usa un token Database Administrator | Rol de lectura creado y usado en las consultas de consumo |
| C1 | Medir la latencia Landing → Gold (objetivo O1, ≤ 15 min) | Requiere un trigger continuo en lugar de `availableNow` | Métrica registrada por micro-lote en el log |
| C2 | Dashboard de FinOps conectado a Astra | Consumo visual de Q1 y Q2 | Tablero con gasto por servicio y anomalías del día |

## Fuera de alcance (Won't, para este MVP)

| Ítem | Motivo |
|---|---|
| Kafka u otra fuente de streaming real | El caso entrega archivos JSONL; el diseño ya separa la fuente del procesamiento |
| Delta Lake / Iceberg (ACID, time travel) | Parquet con overwrite por partición cubre la idempotencia necesaria |
| Modelo de ML supervisado para anomalías | No hay etiquetas y la historia es de 60 días; MAD es explicable y suficiente |
| AstraDB multi-región | Un solo consumidor y una sola región |
| Alertas en tiempo real (email / Slack) | Depende de S1 y C1 |
