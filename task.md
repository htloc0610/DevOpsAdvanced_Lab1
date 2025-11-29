You are an expert Data Engineer specializing in dbt.
Your task is to implement Part 1: DBT Data Models according to the full requirements below.
Follow all instructions strictly, and generate the complete dbt code inside the correct folder structure (models/, staging/, intermediate/, marts/, etc.).
Do NOT change or create folders unless they follow dbt best practices.
All SQL files must contain full column documentation, tests, and descriptions using dbt YAML.
Include model lineage via ref() wherever applicable.
Use AdventureWorks as the source.

Part 1: DBT Data Models (25 points)
Deliverables

Bronze layer models (staging)

Silver layer models (intermediate transformations)

Gold layer models (business-ready marts)

Model documentation + lineage

Requirements
Bronze Layer (8 points)

Create at least 3 staging models:

Extract from at least 3 AdventureWorks source tables

Apply basic cleaning (trim strings, normalize casing, cast types, rename columns, remove null IDs)

Add source freshness checks in sources.yml

Document all columns

Use {{ source() }} in all staging models

Place inside: models/staging/adventureworks/

Silver Layer (8 points)

Create at least 2 intermediate models:

Join multiple bronze/staging models

Implement business logic transformations, such as:

Derived fields

Normalized enums

Surrogate keys

Add tests (unique, not null, accepted values, relationships)

Use {{ ref() }} properly

Place inside: models/intermediate/

Gold Layer (9 points)

Create at least 2 mart models:

Aggregations and KPIs (e.g., sales totals, customer metrics, product performance)

Must be business-ready and optimized

Ensure models are analysis-ready with clean naming conventions

Place inside: models/marts/

Evaluation Criteria

Your generated solution must follow these scoring criteria:

1. Correct SQL syntax & logic (10 points)

No errors, clear joins, correct transformations.

2. Proper layering (8 points)

Staging → Intermediate → Marts

No business logic in staging

No raw sources used in silver/gold

3. Documentation quality (4 points)

YAML with descriptions for:

Models

Columns

Tests

4. Performance (3 points)

Avoid SELECT *

Use surrogate keys

Optimize joins & aggregations

Final Output Requirements

When generating the solution, you must provide:

Full dbt folder structure

All SQL files for Bronze/Silver/Gold layers

YAML documentation & tests

Source configuration + freshness checks

Clear lineage using ref()