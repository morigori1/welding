#requires -version 5.1
param(
  [Parameter(Mandatory=$true)] [string]$XdwRoot,
  [string]$Printer = "CubePDF",
  [string]$AsOf = "",
  [switch]$NoRecurse,
  [string]$ViewerPath = ""
)

$ErrorActionPreference = 'Stop'

function Write-Info($msg) { Write-Host "[INFO] $msg" -ForegroundColor Cyan }
function Write-Warn($msg) { Write-Host "[WARN] $msg" -ForegroundColor Yellow }

# 1) Ensure venv + deps
Write-Info "Ensuring venv + dependencies"
$pythonExe = 'python'
try { & $pythonExe -V | Out-Null } catch { $pythonExe = 'py'; try { & $pythonExe -V | Out-Null } catch { throw "Python not found. Install Python 3.11+ or ensure 'python'/'py' is on PATH." } }
if (-not (Test-Path .venv)) { & $pythonExe -m venv .venv }
$pip = Join-Path .venv 'Scripts/pip.exe'
$py  = Join-Path .venv 'Scripts/python.exe'
& $pip install --upgrade pip > $null
& $pip install -e . | Out-Null
try { & $pip install pyodbc  | Out-Null } catch { Write-Warn "pyodbc install failed; MSSQL import may be skipped." }

# Clear pyc caches to avoid stale imports after edits
try { Remove-Item -Recurse -Force (Join-Path 'src' 'welding_registry' '__pycache__') -ErrorAction SilentlyContinue } catch {}

# 2) Load .env if present
if (Test-Path .env) {
  Write-Info "Loading .env"
  Get-Content .env | Where-Object { $_ -match '=' -and $_ -notmatch '^#' } | ForEach-Object {
    $k,$v = $_.Split('=',2)
    $v = $v.Trim('"')
    [Environment]::SetEnvironmentVariable($k.Trim(), $v)
  }
}

$outPdf = Join-Path 'out' 'pdf'
New-Item -ItemType Directory -Force -Path $outPdf | Out-Null

# 2.5) Ensure roster.xlsx exists (ingest from a source if needed)
$rosterXlsx = Join-Path 'out' 'roster.xlsx'
if (-not (Test-Path $rosterXlsx)) {
  Write-Info "Preparing roster.xlsx via ingest"
  # Pick a reasonable source XLS from data (largest .xls as heuristic)
  $xlsFiles = Get-ChildItem -Recurse -File -Path 'data' -Include *.xls,*.xlsx | Sort-Object Length -Descending
  if ($xlsFiles.Count -gt 0) {
    $srcXls = $xlsFiles[0].FullName
    Write-Info ("Ingesting: {0}" -f $srcXls)
    & $py -m welding_registry ingest "$srcXls" --out out | Out-Null
  } else {
    Write-Warn "No XLS/XLSX found under data; skipping ingest. Place a roster file or run ingest manually."
  }
}

# 3) Workers from SQL Server (optional if creds available)
$haveDb = $env:DB_HOST -and $env:DB_NAME -and $env:DB_USER -and $env:DB_PASSWORD
if ($haveDb) {
  Write-Info "Fetching workers via SQL Server"
  $schema = if ($env:DB_SCHEMA -and $env:DB_SCHEMA.Trim() -ne '') { $env:DB_SCHEMA } else { 'dbo' }
  $wtable = if ($env:DB_WORKER_TABLE -and $env:DB_WORKER_TABLE.Trim() -ne '') { $env:DB_WORKER_TABLE } else { 'T_TM_Worker_T' }
  $driver = if ($env:ODBC_DRIVER -and $env:ODBC_DRIVER.Trim() -ne '') { $env:ODBC_DRIVER } else { 'ODBC Driver 17 for SQL Server' }
  $wargs = @('workers',
    '--host', $env:DB_HOST,
    '--db', $env:DB_NAME,
    '--schema', $schema,
    '--table', $wtable,
    '--user', $env:DB_USER,
    '--password', $env:DB_PASSWORD,
    '--driver', $driver,
    '--out', (Join-Path 'out' 'workers.csv')
  )
  if ($env:DB_ENCRYPT) { $wargs += @('--encrypt', $env:DB_ENCRYPT) }
  if ($env:DB_TRUST_SERVER_CERTIFICATE) { $wargs += @('--trust-server-certificate', $env:DB_TRUST_SERVER_CERTIFICATE) }
  & $py -m welding_registry @wargs
} else {
  Write-Warn "DB_* env not complete; skipping workers import."
}

# 4) XDW/XBD -> PDF with auto-enter helper
Write-Info "Converting XDW/XBD to PDF via DocuWorks Viewer -> $Printer"
$argsX = @('xdw2pdf', $XdwRoot, '--printer', $Printer, '--auto-enter')
if ($NoRecurse) { $argsX += '--no-recurse' }
if ($ViewerPath -and (Test-Path $ViewerPath)) { $argsX += @('--viewer', $ViewerPath) }
& $py -m welding_registry @argsX

# 5) Due calculation with OCR license scan
Write-Info "Computing due list (OCR from $outPdf)"
$dueArgs = @('due', $rosterXlsx,'--licenses-scan', $outPdf,'--out','out/due.csv','--ics','out/due.ics')
if ($AsOf) { $dueArgs += @('--as-of', $AsOf) }
if (Test-Path 'out/workers.csv') { $dueArgs += @('--workers-csv','out/workers.csv') }
& $py -m welding_registry @dueArgs

# 5.5) Fallback: if no PDFs were produced, scan data/ for existing PDFs
$pdfCount = (Get-ChildItem -Recurse -File -Path $outPdf -Filter *.pdf | Measure-Object).Count
if ($pdfCount -eq 0) {
  Write-Warn "No PDFs in $outPdf; falling back to scan data/ for existing PDFs"
  $anyPdfDir = (Get-ChildItem -Recurse -Directory -Path 'data' | Where-Object { (Get-ChildItem -File -Path $_.FullName -Filter *.pdf | Measure-Object).Count -gt 0 } | Select-Object -First 1)
  if ($anyPdfDir) {
    $dueArgs2 = @('due', $rosterXlsx,'--licenses-scan', $anyPdfDir.FullName,'--out','out/due.csv','--ics','out/due.ics')
    if ($AsOf) { $dueArgs2 += @('--as-of', $AsOf) }
    if (Test-Path 'out/workers.csv') { $dueArgs2 += @('--workers-csv','out/workers.csv') }
    & $py -m welding_registry @dueArgs2
  } else {
    Write-Warn "No PDFs found under data/. Skipping OCR merge."
  }
}

# 6) Summary (no PII)
Write-Info "Summary"
if (Test-Path 'out/workers.csv') {
  $n = (Get-Content 'out/workers.csv' | Measure-Object -Line).Lines - 1
  Write-Host ("Workers: {0}" -f [Math]::Max(0,$n))
}
if (Test-Path 'out/roster.csv') {
  $n = (Get-Content 'out/roster.csv' | Measure-Object -Line).Lines - 1
  Write-Host ("Roster: {0}" -f [Math]::Max(0,$n))
}
if (Test-Path 'out/due.csv') {
  $n = (Get-Content 'out/due.csv' | Measure-Object -Line).Lines - 1
  Write-Host ("Due: {0}" -f [Math]::Max(0,$n))
}

Write-Info "Done"
