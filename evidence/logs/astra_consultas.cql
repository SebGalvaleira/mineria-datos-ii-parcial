-- Q1: gasto diario por servicio de una org en un rango
SELECT usage_date, service, daily_cost_usd, eventos, eventos_pico FROM finops.org_daily_usage_by_service
WHERE org_id = 'org_53lc58dr' AND usage_month = '2025-08' AND usage_date >= '2025-08-01' AND usage_date <= '2025-08-07';

-- Q2: anomalías de costo de un día
SELECT org_id, service, daily_cost_usd, eventos_pico, eventos_costo_anomalo FROM finops.cost_anomalies_by_date
WHERE usage_date = '2025-08-23';
