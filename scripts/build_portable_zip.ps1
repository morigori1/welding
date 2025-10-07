param(
    [string]$Python = "",
    [string]$DistRoot = "dist/local-app",
    [string]$ZipPath = "dist/welding-portable.zip"
)

$ErrorActionPreference = "Stop"

function Resolve-Python {
    param([string]$Specified)

    if ($Specified) {
        if (Test-Path $Specified) {
            return (Resolve-Path $Specified).Path
        }
        throw "Python executable not found: $Specified"
    }

    $root = (Get-Location).Path
    $candidates = @(
        (Join-Path -Path $root -ChildPath ".venv\\Scripts\\python.exe"),
        (Join-Path -Path $root -ChildPath ".venv\\Scripts\\python.bat"),
        (Join-Path -Path $root -ChildPath "venv\\Scripts\\python.exe"),
        (Join-Path -Path $root -ChildPath "venv\\Scripts\\python.bat")
    )
    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) {
            return (Resolve-Path $candidate).Path
        }
    }
    return $null
}

$pythonExe = Resolve-Python $Python
$baseArgs = @()
if (-not $pythonExe) {
    $pythonExe = "py"
    $baseArgs = @("-3")
    Write-Host "(py launcher fallback; specify -Python or create .venv to avoid this)"
}

function Invoke-Step {
    param(
        [string]$Message,
        [scriptblock]$Action
    )
    Write-Host "==> $Message"
    & $Action
}

function Invoke-Py {
    param(
        [string[]]$PyArgs,
        [string]$Context = ""
    )
    $allArgs = @()
    if ($baseArgs -and $baseArgs.Count -gt 0) { $allArgs += $baseArgs }
    if ($PyArgs -and $PyArgs.Count -gt 0) { $allArgs += $PyArgs }
    if ($allArgs.Count -eq 0) {
        throw "Python command failed ($Context) because no arguments were provided"
    }
    & $pythonExe @allArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Python command failed ($Context) with exit code $LASTEXITCODE"
    }
}

$root = Get-Location
$buildDir = Join-Path $root 'build/pyinstaller'
if (Test-Path $buildDir) {
    Remove-Item -Recurse -Force $buildDir
}
if (Test-Path $DistRoot) {
    Remove-Item -Recurse -Force $DistRoot
}
New-Item -ItemType Directory -Path $DistRoot | Out-Null

Invoke-Step -Message 'Installing/updating PyInstaller' -Action {
    Invoke-Py -PyArgs @('-m', 'pip', 'install', '-U', 'pyinstaller') -Context 'pip install pyinstaller'
}

New-Item -ItemType Directory -Path $buildDir | Out-Null

$absDist = (Resolve-Path $DistRoot).Path
$absBuild = (Resolve-Path $buildDir).Path
$docsPath = (Resolve-Path (Join-Path $root 'docs')).Path
$tmplPath = (Resolve-Path (Join-Path $root 'src/welding_registry/templates')).Path
$cliScriptPath = (Resolve-Path (Join-Path $root 'src/welding_registry/__main__.py')).Path
$guiScriptPath = (Resolve-Path (Join-Path $root 'scripts/gui_launcher.py')).Path

Invoke-Step -Message 'Building welding-cli.exe' -Action {
    $code = @"
import PyInstaller.__main__
PyInstaller.__main__.run([
    '--noconfirm',
    '--clean',
    '--collect-submodules', 'welding_registry',
    '--collect-data', 'welding_registry.webapp',
    '--distpath', r'$absDist',
    '--workpath', r'$absBuild',
    '--specpath', r'$absBuild',
    '--add-data', r'$docsPath;docs',
    '--add-data', r'$tmplPath;welding_registry/templates',
    '--name', 'welding-cli',
    '--console', r'$cliScriptPath',
])
"@
    Invoke-Py -PyArgs @('-c', $code) -Context 'pyinstaller welding-cli'
}

Invoke-Step -Message 'Building welding-gui.exe' -Action {
    $code = @"
import PyInstaller.__main__
PyInstaller.__main__.run([
    '--noconfirm',
    '--clean',
    '--collect-submodules', 'welding_registry',
    '--collect-data', 'welding_registry.webapp',
    '--distpath', r'$absDist',
    '--workpath', r'$absBuild',
    '--specpath', r'$absBuild',
    '--add-data', r'$docsPath;docs',
    '--add-data', r'$tmplPath;welding_registry/templates',
    '--name', 'welding-gui',
    '--windowed',
    r'$guiScriptPath',
])
"@
    Invoke-Py -PyArgs @('-c', $code) -Context 'pyinstaller welding-gui'
}

$warehouseDir = Join-Path $DistRoot 'warehouse'
$warehouseFile = Join-Path $warehouseDir 'local.duckdb'
if (Test-Path $warehouseDir) {
    Remove-Item -Recurse -Force $warehouseDir
}
New-Item -ItemType Directory -Path $warehouseDir | Out-Null

$sourceDir = Join-Path $root 'warehouse'
$sourceDb = Join-Path $sourceDir 'local.duckdb'
if (Test-Path $sourceDb) {
    Copy-Item -Path $sourceDb -Destination $warehouseFile -Force
}
else {
    Invoke-Py -PyArgs @('-c', "import duckdb, pathlib; p = pathlib.Path(r'${warehouseFile}'); p.parent.mkdir(parents=True, exist_ok=True); duckdb.connect(str(p)).close()") -Context 'init duckdb file'
}

$readmePath = Join-Path $DistRoot 'README_portable.txt'
$readmeLines = @(
    'Welding Registry Portable Package',
    '=================================',
    '',
    'Contents',
    '--------',
    '- welding-cli\welding-cli.exe    : command-line interface (`welding-cli.exe --help`).',
    '- welding-gui\welding-gui.exe    : Tkinter desktop GUI launcher.',
    '',
    'Usage',
    '-----',
    '1. Extract the ZIP somewhere without Japanese characters in the path if possible.',
    '2. Double-click `welding-gui.exe` for the GUI, or run `welding-cli.exe` from PowerShell/Command Prompt.',
    '3. DuckDB database is bundled at `warehouse/local.duckdb` (replace this file if needed).',
    '',
    'Notes',
    '-----',
    '- The bundle ships Python 3.11 and all dependencies (pandas, openpyxl, duckdb, Flask, etc.).',
    '- Tesseract OCR is not included; install it separately and set `TESSERACT_CMD` if you need OCR.',
    '- On first launch Windows Defender SmartScreen may prompt because the EXE is unsigned.'
)
$readme = $readmeLines -join [Environment]::NewLine
Set-Content -Path $readmePath -Value $readme -Encoding UTF8

if (Test-Path $ZipPath) {
    Remove-Item -Force $ZipPath
}

$items = Get-ChildItem -Path $DistRoot
Compress-Archive -Path $items.FullName -DestinationPath $ZipPath -Force
Write-Host 'Portable ZIP created:' (Resolve-Path $ZipPath)






