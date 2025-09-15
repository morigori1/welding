<#
Optional helper to run the app after setup.
Usage:
  .\scripts\run_app.ps1
#>
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
& .\.venv\Scripts\Activate.ps1
python -m welding_registry app --duckdb out\registry.duckdb --review-db warehouse\review.sqlite

