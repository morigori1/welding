param(
  [Parameter(Mandatory=$true)][string]$InputDir,
  [string]$OutDir = "out/pdf",
  [string]$Printer = "Microsoft Print to PDF",
  [switch]$Recurse,
  [switch]$DryRun,
  [int]$MaxCount = 0
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Find-AutoHotkey {
  $candidates = @()
  if ($env:ProgramFiles)        { $candidates += (Join-Path $env:ProgramFiles 'AutoHotkey\AutoHotkey.exe') }
  if ($env:ProgramFiles)        { $candidates += (Join-Path $env:ProgramFiles 'AutoHotkey\v1\AutoHotkey.exe') }
  if ($env:ProgramFiles)        { $candidates += (Join-Path $env:ProgramFiles 'AutoHotkey\v2\AutoHotkey.exe') }
  if (${env:ProgramFiles(x86)}) { $candidates += (Join-Path ${env:ProgramFiles(x86)} 'AutoHotkey\AutoHotkey.exe') }
  foreach ($p in $candidates) { if (Test-Path $p) { return $p } }
  return 'AutoHotkey.exe'
}

function Get-DefaultPrinterName {
  (Get-CimInstance -ClassName Win32_Printer | Where-Object { $_.Default -eq $true } | Select-Object -First 1 -ExpandProperty Name)
}

function Set-DefaultPrinterName([string]$name) {
  $rc = (Start-Process -FilePath rundll32.exe -ArgumentList "printui.dll,PrintUIEntry /y /n `"$name`"" -Wait -PassThru).ExitCode
  if ($rc -ne 0) { throw "Failed to set default printer: $name (rc=$rc)" }
}

if (-not (Test-Path $InputDir)) { throw "InputDir not found: $InputDir" }
$absInputRoot = (Resolve-Path -LiteralPath $InputDir).Path.TrimEnd('\')
$absOutRoot = [System.IO.Path]::GetFullPath($OutDir)
New-Item -ItemType Directory -Path $absOutRoot -Force | Out-Null

$ahk = Find-AutoHotkey
Write-Host "Using AutoHotkey: $ahk"

$opt = @{ File = $true; Recurse = [bool]$Recurse }
$files = @(Get-ChildItem -Path $absInputRoot -Filter *.xdw @opt) + @(Get-ChildItem -Path $absInputRoot -Filter *.xbd @opt)
$files = $files | Sort-Object FullName -Unique
if ($MaxCount -gt 0) { $files = $files | Select-Object -First $MaxCount }
if (-not $files) { Write-Host 'No .xdw/.xbd files found.'; exit 0 }

$orig = Get-DefaultPrinterName
Write-Host "Default printer: $orig" -ForegroundColor DarkGray
Write-Host "Switching default to: $Printer" -ForegroundColor Cyan
if (-not $DryRun) { Set-DefaultPrinterName -name $Printer }

$log = Join-Path $absOutRoot "conversion.log"
"Start: $(Get-Date -Format s) | TargetPrinter=$Printer" | Out-File -FilePath $log -Encoding UTF8

foreach ($f in $files) {
  $full = (Resolve-Path -LiteralPath $f.FullName).Path
  # Robust relative path via substring
  if ($full.StartsWith($absInputRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
    $rel = $full.Substring($absInputRoot.Length).TrimStart('\','/')
  } else {
    $rel = Split-Path -Leaf $full
  }
  $outPathAbs = Join-Path $absOutRoot $rel
  $outPathAbs = [System.IO.Path]::ChangeExtension($outPathAbs, '.pdf')
  $outDir = Split-Path $outPathAbs -Parent
  New-Item -ItemType Directory -Path $outDir -Force | Out-Null
  Write-Host "Converting: $full" -ForegroundColor Green
  Write-Host " -> $outPathAbs" -ForegroundColor Green

  if ($DryRun) {
    "DRY: $full -> $outPathAbs" | Add-Content $log
    continue
  }

  $script = 'scripts\xdw_to_pdf.ahk'
  if ( ("{0}" -f $ahk) -match '\\v2\\AutoHotkey\.exe' ) { $script = 'scripts\xdw_to_pdf_v2.ahk' }
  $proc = Start-Process -FilePath $ahk -ArgumentList @($script, $full, $outPathAbs) -PassThru -Wait
  $code = $proc.ExitCode
  if ($code -ne 0) {
    Write-Warning "Failed (code=$code): $full"
    "FAIL code=$code | $full" | Add-Content $log
  } else {
    "OK | $full -> $outPathAbs" | Add-Content $log
  }
}

Write-Host "Restoring default printer: $orig" -ForegroundColor Cyan
if (-not $DryRun) { Set-DefaultPrinterName -name $orig }
"End: $(Get-Date -Format s)" | Add-Content $log

