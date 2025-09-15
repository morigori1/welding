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
  return 'AutoHotkey.exe' # rely on PATH
}

function Get-DefaultPrinterName {
  $def = Get-CimInstance -ClassName Win32_Printer | Where-Object { $_.Default -eq $true } | Select-Object -First 1 -ExpandProperty Name
  return $def
}

function Set-DefaultPrinterName([string]$name) {
  $cmd = "printui.dll,PrintUIEntry"
  $args = "/y /n `"$name`""
  $rc = (Start-Process -FilePath rundll32.exe -ArgumentList "$cmd $args" -Wait -PassThru).ExitCode
  if ($rc -ne 0) { throw "Failed to set default printer: $name (rc=$rc)" }
}

if (-not (Test-Path $InputDir)) { throw "InputDir not found: $InputDir" }
$absOutRoot = [System.IO.Path]::GetFullPath($OutDir)
New-Item -ItemType Directory -Path $absOutRoot -Force | Out-Null
$root = (Resolve-Path -LiteralPath $InputDir).Path

$ahk = Find-AutoHotkey
Write-Host "Using AutoHotkey: $ahk"

$opt = @{ File = $true; Recurse = [bool]$Recurse }
$files = @()
$files += Get-ChildItem -Path $InputDir -Filter *.xdw @opt
$files += Get-ChildItem -Path $InputDir -Filter *.xbd @opt
$files = $files | Sort-Object FullName -Unique
if ($MaxCount -gt 0) { $files = $files | Select-Object -First $MaxCount }
if (-not $files) { Write-Host 'No .xdw/.xbd files found.'; exit 0 }

# Switch default printer to desired target for the session
$orig = Get-DefaultPrinterName
Write-Host "Default printer: $orig" -ForegroundColor DarkGray
Write-Host "Switching default to: $Printer" -ForegroundColor Cyan
if (-not $DryRun) { Set-DefaultPrinterName -name $Printer }

$log = Join-Path $OutDir "conversion.log"
"Start: $(Get-Date -Format s) | TargetPrinter=$Printer" | Out-File -FilePath $log -Encoding UTF8

foreach ($f in $files) {
  $full = (Resolve-Path -LiteralPath $f.FullName).Path
  try {
    $rel  = [System.IO.Path]::GetRelativePath($root, $full)
  } catch {
    $uRoot = [Uri]::new(($root.TrimEnd('\') + '\\'))
    $uFull = [Uri]::new($full)
    $rel = [Uri]::UnescapeDataString($uRoot.MakeRelativeUri($uFull).ToString())
    $rel = $rel -replace '/'``n  $rel = $rel -replace '^[.]+[\\/]+'', '''`r`n, '\\'
  }
  $outPathAbs = Join-Path $absOutRoot $rel
  $outPathAbs = [System.IO.Path]::ChangeExtension($outPathAbs, '.pdf')
  $outDir = Split-Path $outPathAbs -Parent
  New-Item -ItemType Directory -Path $outDir -Force | Out-Null
  Write-Host "Converting: $($f.FullName)" -ForegroundColor Green
  Write-Host " -> $outPathAbs" -ForegroundColor Green

  if ($DryRun) {
    "DRY: $($f.FullName) -> $outPathAbs" | Add-Content $log
    continue
  }

  $script = 'scripts\xdw_to_pdf.ahk'
  if ( ("{0}" -f $ahk) -match '\\v2\\AutoHotkey\.exe' ) { $script = 'scripts\xdw_to_pdf_v2.ahk' }
  $ahkArgs = @($script, $f.FullName, $outPathAbs)
  $proc = Start-Process -FilePath $ahk -ArgumentList $ahkArgs -PassThru -Wait
  $code = $proc.ExitCode
  if ($code -ne 0) {
    Write-Warning "Failed (code=$code): $($f.FullName)"
    "FAIL code=$code | $($f.FullName)" | Add-Content $log
  } else {
    "OK | $($f.FullName) -> $outPathAbs" | Add-Content $log
  }
}

Write-Host "Restoring default printer: $orig" -ForegroundColor Cyan
if (-not $DryRun) { Set-DefaultPrinterName -name $orig }
"End: $(Get-Date -Format s)" | Add-Content $log

