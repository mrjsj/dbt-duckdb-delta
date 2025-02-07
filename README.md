# To run


```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)/dbt_duckdb_delta" && dbt run --profiles-dir ./dbt_duckdb_delta --project-dir ./dbt_duckdb_delta
```

Alternative with multiple threads (fails)

```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)/dbt_duckdb_delta" && dbt run --profiles-dir ./dbt_duckdb_delta --project-dir ./dbt_duckdb_delta --threads 2
```