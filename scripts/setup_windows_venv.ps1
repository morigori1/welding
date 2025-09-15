<#
Usage:
  PowerShell (管理者不要) で C:\welding に移動してから実行:
    .\scripts\setup_windows_venv.ps1

What it does:
  - Remove broken .venv / .venv_cli if present
  - Create new venv with py -3.13 (fallback to 3.12 / python)
  - Activate venv (execution policy bypass for this process)
  - Install editable package + runtime deps
  - Verify core imports (Flask/duckdb/pandas/pdfplumber)
Stops right before launching the app.
#>
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Write-Info($msg) { Write-Host (">>> " + $msg) -ForegroundColor Cyan }
function Write-Warn($msg) { Write-Host ("[! ] " + $msg) -ForegroundColor Yellow }

Push-Location (Split-Path -Parent $MyInvocation.MyCommand.Path) | Out-Null
try {
  Set-Location (Resolve-Path "..\")
} finally {
  Pop-Location | Out-Null
}

Write-Info "Working directory: $(Get-Location)"

if ($env:VIRTUAL_ENV) { Write-Info "Deactivating current venv"; deactivate }

Write-Info "Removing old virtualenvs if exist"
Remove-Item -Recurse -Force .\.venv -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .\.venv_cli -ErrorAction SilentlyContinue

# Choose Python
function Test-PyVersion($spec) {
  try { & py $spec -c "import sys;print(sys.version)" | Out-Null; return $true } catch { return $false }
}

$pySpec = ''
if (Get-Command py -ErrorAction SilentlyContinue) {
  if (Test-PyVersion '-3.13') { $pySpec = '-3.13' }
  elseif (Test-PyVersion '-3.12') { $pySpec = '-3.12' }
}

Write-Info "Creating virtualenv (.venv)"
if ($pySpec) {
  & py $pySpec -m venv .venv
} else {
  & python -m venv .venv
}

Write-Info "Activating venv"
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
& .\.venv\Scripts\Activate.ps1
Write-Info "Python: $(python -V)"

Write-Info "Upgrading pip/setuptools/wheel"
python -m pip install -U pip setuptools wheel

Write-Info "Installing package in editable mode (. = C:\\welding)"
pip install -e .

Write-Info "Verifying core imports (Flask/duckdb/pandas/pdfplumber)"
$code = @"
import sys
mods = ['flask','duckdb','pandas','pdfplumber']
missing = []
for m in mods:
    try:
        __import__(m)
    except Exception as e:
        missing.append((m, str(e)))
print('python:', sys.executable)
if missing:
    print('MISSING:', missing)
    raise SystemExit(1)
print('OK')
"@
$tf = [System.IO.Path]::Combine([System.IO.Path]::GetTempPath(), [System.IO.Path]::GetRandomFileName() + '.py')
[System.IO.File]::WriteAllText($tf, $code)
& python $tf
Remove-Item $tf -Force

Write-Host "\nAll set. To run the app next:" -ForegroundColor Green
Write-Host "  .\\.venv\\Scripts\\Activate.ps1" -ForegroundColor Green
Write-Host "  python -m welding_registry app --duckdb out\\registry.duckdb --review-db warehouse\\review.sqlite" -ForegroundColor Green
