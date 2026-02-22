## Team
- **ilyas DAHAOUI**
- **Mohammed Adam KHALI**
  
## Key Features

### From Lab 1
- **Continuation Tokens**: Reviews extraction uses pagination tokens to fetch maximum available data
- **Append Mode**: Reviews are written incrementally to prevent data loss if extraction crashes
- **Robust Error Handling**: Pipeline continues even if individual apps fail
- **Flexible JSON Loading**: Supports both JSON array and JSONL formats

### New in Lab 2
- **Incremental Loading**: New app reviews can be appended without full refresh; duplicates are merged by `review_id`.
- **Slowly Changing Dimension (SCD Type 2)**: App metadata changes are versioned with `start_date`, `end_date`, and `current_flag` columns, producing a history table (`apps_metadata_scd2.json`).
- **Data Quality Checks**: Basic validators report missing IDs, incorrect types, and produce console warnings prior to aggregation.
- **Automated Testing**: PyTest suite covers utility functions, quality checks, and end‑to‑end pipeline behaviour.
- **Star-Schema Export**: A helper can generate dimension (`dim_apps`, `dim_categories`, `dim_developers`, `dim_date`) and fact (`fact_reviews`) tables matching the provided schema image.

### Pipeline Capabilities
- **Data Quality**: Handles missing values, type conversions, date parsing and executes custom quality rules
- **Standardization**: Normalizes field names and data formats
- **Aggregation**: Calculates review metrics (avg score, rating distribution, reply rates)
- **Joins**: Combines dimension (apps) and fact (reviews) data using current snapshot from SCD2
- **Modular Design**: Each stage can be tested independently, and configuration is centralized

### dbt & DuckDB (Lab 2 extension)
The pipeline can optionally be rebuilt using dbt and DuckDB. A skeleton dbt project
lives under `dbt_project/` with sample staging and mart models that mirror the
schema described above.  To use it:

1. **Install dependencies** in the same virtual environment:
   ```powershell
   pip install duckdb dbt-core dbt-duckdb
   ```
2. **Verify installation**:
   ```powershell
   dbt --version
   ```
   You should see `duckdb` listed under `installed plugins`.
3. **Configure profiles** – the included `dbt_project/profiles.yml` sets up a
   local DuckDB file at `dbt_project/data/dbt.duckdb`.  Export
   `DBT_PROFILES_DIR=dbt_project` before running dbt, or copy the file to
   `~/.dbt/profiles.yml`.
4. **Run dbt** from the root of the workspace:
   ```powershell
   cd dbt_project
   dbt deps          # install any packages (none required here)
   dbt run --vars "{'apps_metadata_path':'../DATA/raw/apps_metadata.json','apps_reviews_path':'../DATA/raw/apps_reviews.json'}"
   dbt test          # run any built-in tests (none defined yet)
   ```
5. **Inspect results** – the models create DuckDB tables in
   `dbt_project/data/dbt.duckdb`.  You can query them using `duckdb` CLI or
   connect a BI tool.

The existing Python pipeline remains fully functional and the dbt project is
provided as a demonstration of the instructions above.  You are free to
customize or extend the dbt models as required by the lab.

