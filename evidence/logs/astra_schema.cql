CREATE TABLE IF NOT EXISTS finops.org_daily_usage_by_service (
    org_id text, usage_month text, usage_date date, service text,
    daily_cost_usd double, requests double, cpu_hours double, storage_gb_hours double,
    genai_tokens bigint, carbon_kg double,
    eventos int, eventos_costo_anomalo int, eventos_pico int,
    industry text, plan_tier text, updated_at timestamp,
    PRIMARY KEY ((org_id, usage_month), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);

CREATE TABLE IF NOT EXISTS finops.cost_anomalies_by_date (
    usage_date date, org_id text, service text,
    daily_cost_usd double, eventos int, eventos_costo_anomalo int, eventos_pico int,
    industry text, plan_tier text, updated_at timestamp,
    PRIMARY KEY ((usage_date), org_id, service)
);
