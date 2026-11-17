-- Consultas de serving · ejecutadas en el CQL Console de Astra (capturas en evidence/e2/capturas/)
-- Los valores corresponden a la corrida documentada: org con mayor gasto y día con más anomalías.

-- Q1 · gasto diario por servicio de una org en un rango (resultado: 29 filas, 419,1986 USD = Gold)
SELECT usage_date, service, daily_cost_usd, eventos, eventos_pico
FROM finops.org_daily_usage_by_service
WHERE org_id = 'org_53lc58dr' AND usage_month = '2025-08'
  AND usage_date >= '2025-08-01' AND usage_date <= '2025-08-07';

-- Q2 · anomalías de costo de un día (resultado: 11 filas)
SELECT org_id, service, daily_cost_usd, eventos_pico, eventos_costo_anomalo
FROM finops.cost_anomalies_by_date
WHERE usage_date = '2025-08-23';

-- Contraejemplo · filtrar por una columna que no es clave: Cassandra la rechaza (InvalidRequest, code=2200)
-- "PRIMARY KEY column "service" cannot be restricted as preceding column "usage_date" is not restricted"
SELECT * FROM finops.org_daily_usage_by_service WHERE service = 'genai';

-- Esquema tal como quedó en Astra
DESCRIBE TABLE finops.org_daily_usage_by_service;
