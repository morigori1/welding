param(
  [Parameter(Mandatory=$true)][string]$InputDir,
  [string]$Printer = 'DocuWorks PDF',
  [switch]$Recurse,
  [switch]$DryRun
)

function Find-DwViewer {
  $candidates = @(
    'C:\Program Files\FUJIFILM Business Innovation\DocuWorks\dwviewer.exe',
    'C:\Program Files (x86)\FUJIFILM Business Innovation\DocuWorks\dwviewer.exe',
    'C:\Program Files\Fuji Xerox\DocuWorks\dwviewer.exe',
    'C:\Program Files (x86)\Fuji Xerox\DocuWorks\dwviewer.exe'
  )
  foreach ($p in $candidates) { if (Test-Path $p) { return $p } }
  # Registry lookup (InstallPath)
  $regKeys = @(
    'HKLM:\SOFTWARE\FUJIFILM Business Innovation\DocuWorks',
    'HKLM:\SOFTWARE\WOW6432Node\FUJIFILM Business Innovation\DocuWorks',
    'HKLM:\SOFTWARE\Fuji Xerox\DocuWorks',
    'HKLM:\SOFTWARE\WOW6432Node\Fuji Xerox\DocuWorks'
  )
  foreach ($k in $regKeys) {
    if (Test-Path $k) {
      $ip = (Get-ItemProperty -Path $k -ErrorAction SilentlyContinue).InstallPath
      if ($ip) {
        $exe = Join-Path $ip 'dwviewer.exe'
        if (Test-Path $exe) { return $exe }
      }
    }
  }
  return $null
}

$dw = Find-DwViewer
if (-not $dw) {
  Write-Error 'DocuWorks Viewer (dwviewer.exe) not found. Install DocuWorks Viewer/Desk and retry.'
  exit 1
}

$opt = @{}
if ($Recurse) { $opt.Recurse = $true }
$files = Get-ChildItem -Path $InputDir -Include *.xdw,*.xbd @opt | Sort-Object FullName
if (-not $files) { Write-Host 'No .xdw/.xbd files found.'; exit 0 }

Write-Host "Using viewer: $dw" -ForegroundColor Cyan
Write-Host "Target printer: $Printer" -ForegroundColor Cyan
Write-Host 'NOTE: Configure the printer for auto-save to avoid Save As prompts.' -ForegroundColor Yellow

foreach ($f in $files) {
  $args = "/pt `"$Printer`" `"$($f.FullName)`""
  if ($DryRun) {
    Write-Host "DRY: $dw $args"
  } else {
    Write-Host "Printing: $($f.Name)" -NoNewline; Write-Host " -> $Printer"
    Start-Process -FilePath $dw -ArgumentList $args -WindowStyle Minimized -Wait
  }
}

