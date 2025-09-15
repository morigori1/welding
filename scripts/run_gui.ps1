param(
  [string]$DuckDB = "warehouse/local.duckdb"
)

Write-Host "Launching local GUI with DuckDB: $DuckDB"
py -3.11 -m welding_registry gui --duckdb "$DuckDB"

