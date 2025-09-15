param(
  [Parameter(Mandatory=$true)][string]$InputDir,
  [Parameter(Mandatory=$true)][string]$PdfDir,
  [Parameter(Mandatory=$true)][string]$XlsPath,
  [string]$Sheet = 'P1',
  [int]$HeaderRow = 7,
  [int]$WindowDays = 120,
  [string]$DueCsv = 'out/due.csv',
  [string]$IcsPath = 'out/expiry.ics',
  [int]$PollSeconds = 10,
  [int]$TimeoutMinutes = 0
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Count-Source {
  (Get-ChildItem -Path $InputDir -Filter *.xdw -File -Recurse).Count +
  (Get-ChildItem -Path $InputDir -Filter *.xbd -File -Recurse).Count
}

function Count-Pdf {
  (Get-ChildItem -Path $PdfDir -Filter *.pdf -File -Recurse -ErrorAction SilentlyContinue).Count
}

if (-not (Test-Path $PdfDir)) { New-Item -ItemType Directory -Path $PdfDir -Force | Out-Null }
$expected = Count-Source
Write-Host "Monitor started. Expected PDFs: $expected"

$sw = [Diagnostics.Stopwatch]::StartNew()
while ($true) {
  $done = Count-Pdf
  Write-Host ("Progress: {0}/{1} PDFs | Elapsed {2:n0}s" -f $done, $expected, $sw.Elapsed.TotalSeconds)
  if ($done -ge $expected -and $expected -gt 0) { break }
  if ($TimeoutMinutes -gt 0 -and $sw.Elapsed.TotalMinutes -ge $TimeoutMinutes) {
    Write-Warning "Timeout reached before completion. Proceeding to due generation with available PDFs."
    break
  }
  Start-Sleep -Seconds $PollSeconds
}

Write-Host "Generating due list and calendar from: $XlsPath"
$args = @('due','--sheet', $Sheet, '--header-row', $HeaderRow, '--licenses-scan', $PdfDir, '--window', $WindowDays, '--out', $DueCsv, '--ics', $IcsPath, '--expiry-from', 'issue_date', '--valid-years', 3, '--', $XlsPath)
python -m welding_registry @args
$rc = $LASTEXITCODE
if ($rc -ne 0) { Write-Warning "due generation exited with code $rc" }
Write-Host "Done. Output: $DueCsv ; $IcsPath"
