-- Cloud Provider Analytics · capa de serving en AstraDB (Cassandra 4.0)
-- Base: cloud_provider_analytics (Serverless non-vector) · keyspace: finops
-- En Astra el keyspace se crea desde la consola (Add keyspace): CREATE KEYSPACE no está permitido por CQL.
-- La replicación la administra Astra dentro de la región; no se declara acá.
-- Ejecutado por notebooks/e2/E2_06_serving_astradb.ipynb (celda 5).

-- Q1 · ¿Cuánto gastó la org X por servicio y por día en un rango de fechas?
-- Partición (org_id, usage_month): máximo 31 días × 6 servicios = 186 filas, acotada aunque pasen años.
-- Clustering usage_date DESC: el rango de fechas es una lectura secuencial dentro de la partición.
CREATE TABLE IF NOT EXISTS finops.org_daily_usage_by_service (
    org_id text, usage_month text, usage_date date, service text,
    daily_cost_usd double, requests double, cpu_hours double, storage_gb_hours double,
    genai_tokens bigint, carbon_kg double,
    eventos int, eventos_costo_anomalo int, eventos_pico int,
    industry text, plan_tier text, updated_at timestamp,
    PRIMARY KEY ((org_id, usage_month), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);

-- Q2 · ¿Qué orgs y servicios tuvieron costos anómalos el día D?
-- Partición usage_date: todas las anomalías del día en una sola lectura (promedio ≈ 4 filas, máximo 11).
-- El costo NO es clustering: cambia al recalcular y dejaría filas huérfanas (el upsert dejaría de ser idempotente).
CREATE TABLE IF NOT EXISTS finops.cost_anomalies_by_date (
    usage_date date, org_id text, service text,
    daily_cost_usd double, eventos int, eventos_costo_anomalo int, eventos_pico int,
    industry text, plan_tier text, updated_at timestamp,
    PRIMARY KEY ((usage_date), org_id, service)
);
