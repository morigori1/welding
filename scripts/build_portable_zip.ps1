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

    $root = Get-Location
    $candidates = @(
        Join-Path $root ".venv\\Scripts\\python.exe",
        Join-Path $root ".venv\\Scripts\\python.bat",
        Join-Path $root "venv\\Scripts\\python.exe",
        Join-Path $root "venv\\Scripts\\python.bat"
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
        [string[]]$Args,
        [string]$Context = ""
    )
    $allArgs = $baseArgs + ($Args ?? @())
    $psi = @{
        FilePath = $pythonExe
        ArgumentList = $allArgs
        Wait = $true
        NoNewWindow = $true
        PassThru = $true
    }
    $proc = Start-Process @psi
    if ($proc.ExitCode -ne 0) {
        throw "Python command failed ($Context) with exit code $($proc.ExitCode)"
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
    Invoke-Py -Args @('-m', 'pip', 'install', '-U', 'pyinstaller') -Context 'pip install pyinstaller'
}

$commonArgs = @(
    '--noconfirm',
    '--clean',
    '--collect-submodules', 'welding_registry',
    '--distpath', $DistRoot,
    '--workpath', $buildDir,
    '--specpath', $buildDir,
    '--add-data', 'docs' + [IO.Path]::PathSeparator + 'docs',
    '--add-data', 'src/welding_registry/templates' + [IO.Path]::PathSeparator + 'welding_registry/templates'
)

Invoke-Step -Message 'Building welding-cli.exe' -Action {
    $cliArgs = @('-m', 'PyInstaller') + $commonArgs + @('--name', 'welding-cli', '--console', 'src/welding_registry/__main__.py')
    Invoke-Py -Args $cliArgs -Context 'pyinstaller welding-cli'
}

Invoke-Step -Message 'Building welding-gui.exe' -Action {
    $guiArgs = @('-m', 'PyInstaller') + $commonArgs + @('--name', 'welding-gui', '--windowed', 'scripts/gui_launcher.py')
    Invoke-Py -Args $guiArgs -Context 'pyinstaller welding-gui'
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
    '3. DuckDB data lives alongside the executable by default (`warehouse/local.duckdb`).',
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





