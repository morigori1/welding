SQL Server: Fetching Workers

Usage examples:

- Via environment variables:

  Set:
  - `DB_HOST=192.9.200.253\OBCINSTANCE4`
  - `DB_NAME=obc4mco1`
  - `DB_USER=tokki`
  - `DB_PASSWORD=tokki`
  - `DB_SCHEMA=dbo` (optional)
  - `ODBC_DRIVER=ODBC Driver 17 for SQL Server` (optional)
  - `DB_ENCRYPT=yes` (optional)
  - `DB_TRUST_SERVER_CERTIFICATE=yes` (optional)

  Then run:

  `python -m welding_registry workers --table Worker --where "Active = 1" --out out/workers.csv`

- Pass options directly:

  `python -m welding_registry workers --host "192.9.200.253\\OBCINSTANCE4" --db obc4mco1 --schema dbo --table Worker --user tokki --password tokki --out out/workers.csv`

Filter due list to active workers by name/employee_id:

`python -m welding_registry due roster.xlsx --workers-csv out/workers.csv --out out/due.csv`

Enrich roster with birth date/year from WorkerT:

- After you have `roster` (e.g., via `ingest-vertical`) and either `workers` table in DuckDB or a `workers.csv`:

  `python -m welding_registry enrich --duckdb warehouse/local.duckdb --workers-csv out/workers.csv`

- Output: DuckDB table `roster_enriched` with `birth_date` and `birth_year_west` attached (join by `employee_id` if available, otherwise a normalized name key).

Notes:
- This feature requires `pyodbc` and a SQL Server ODBC driver (e.g., ODBC Driver 17).
- The CLI avoids logging PII; it prints only row counts.
