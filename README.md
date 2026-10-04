# Cloud Provider Analytics — Proyecto Integrador

Minería de Datos II · ISTEA 2C 2026 · Prof. Diego Mosquera

Pipeline de datos para un proveedor de nube: ingesta batch y streaming, Data Lake (Landing / Bronze / Silver / Gold) en Parquet y marts para FinOps, Soporte y Producto servidos en Cassandra/AstraDB.

## Estado

| Entrega | Fecha | Estado |
|---|---|---|
| 1 · Diseño y fundación de datos | 07/10/2026 19:00 | En curso |
| 2 · Pipeline ejecutable | 18/11/2026 19:00 | Pendiente |
| Final · MVP y defensa | 09/12/2026 | Pendiente |

## Estructura

```
README.md        este archivo
DECISIONS.md     registro de decisiones (ADR)
docs/            documento de diseño, diagrama (arquitectura_v1.dot + img/)
data/            instrucciones para obtener el dataset
notebooks/       exploración de datos
src/             código de ingesta y procesamiento (entrega 2)
evidence/        capturas y salidas
```

## Documentación

- [Documento de diseño · Entrega 1](docs/diseno_entrega1.md)
- [Decisiones](DECISIONS.md)

## Notebooks

| Notebook | Respalda |
|---|---|
| `notebooks/01_exploracion_landing.ipynb` | §3 Inventario y perfil de fuentes |
| `notebooks/02_cifras_5v_y_arquitectura.ipynb` | §2 5V y §4.3 simulación de watermark |

Instalar dependencias con `pip install -r requirements.txt`. Para regenerar el diagrama hace falta Graphviz: `python docs/build_diagrama.py`.

## Datos

Ver [data/README.md](data/README.md).

## Convenciones

- Nombres en `snake_case`, tablas con prefijo de zona: `bronze_`, `silver_`, `gold_`.
- Rutas del lake: `datalake/<zona>/<tabla>/<columna_particion>=<valor>/`.
- Fechas en UTC, formato ISO 8601. Montos en USD salvo indicación.
- Los archivos de `landing/` son inmutables.
