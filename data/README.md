# Datos

Dataset sintético provisto por la cátedra: `cloud_provider_challenge_dataset_v1` (~13 MB), incluido en este repositorio.

```
data/datalake/landing/
├── customers_orgs.csv
├── users.csv
├── resources.csv
├── support_tickets.csv
├── marketing_touches.csv
├── nps_surveys.csv
├── billing_monthly.csv
└── usage_events_stream/events_part_0000.jsonl … events_part_0119.jsonl
```

Los archivos de `landing/` son inmutables: no se modifican. Las zonas `bronze/`, `silver/`, `gold/` y `quarantine/` las genera el pipeline y no se versionan (ver `.gitignore`).
