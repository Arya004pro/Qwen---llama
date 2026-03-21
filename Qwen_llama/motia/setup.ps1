# Motia Sales Analytics - Windows Setup Script (PowerShell)
# Usage: powershell -ExecutionPolicy Bypass -File setup.ps1

$ErrorActionPreference = "Stop"
$MOTIA_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$BIN_DIR = Join-Path $MOTIA_DIR "bin"

Write-Host ""
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "  Motia Sales Analytics - Windows Setup  " -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan

# Create bin directory
if (-not (Test-Path $BIN_DIR)) {
    New-Item -ItemType Directory -Path $BIN_DIR -Force | Out-Null
}

# Step 1: Download iii engine
Write-Host ""
Write-Host "[1/5] Downloading iii engine v0.9.0..." -ForegroundColor Yellow
$iiiZip = Join-Path $BIN_DIR "iii.zip"
$iiiExe = Join-Path $BIN_DIR "iii.exe"

if (Test-Path $iiiExe) {
    Write-Host "  OK: iii.exe already exists" -ForegroundColor Green
} else {
    $iiiUrl = "https://github.com/iii-hq/iii/releases/download/iii/v0.9.0/iii-x86_64-pc-windows-msvc.zip"
    Write-Host "  Downloading from GitHub..."
    Invoke-WebRequest -Uri $iiiUrl -OutFile $iiiZip -UseBasicParsing
    Expand-Archive -Path $iiiZip -DestinationPath $BIN_DIR -Force
    Remove-Item $iiiZip -ErrorAction SilentlyContinue
    Write-Host "  OK: iii engine downloaded" -ForegroundColor Green
}

# Step 2: Download iii-cli
Write-Host ""
Write-Host "[2/5] Downloading iii-cli v0.9.0..." -ForegroundColor Yellow
$cliZip = Join-Path $BIN_DIR "iii-cli.zip"
$cliExe = Join-Path $BIN_DIR "iii-cli.exe"

if (Test-Path $cliExe) {
    Write-Host "  OK: iii-cli.exe already exists" -ForegroundColor Green
} else {
    $cliUrl = "https://github.com/iii-hq/iii/releases/download/iii/v0.9.0/iii-cli-x86_64-pc-windows-msvc.zip"
    Write-Host "  Downloading from GitHub..."
    Invoke-WebRequest -Uri $cliUrl -OutFile $cliZip -UseBasicParsing
    Expand-Archive -Path $cliZip -DestinationPath $BIN_DIR -Force
    Remove-Item $cliZip -ErrorAction SilentlyContinue
    Write-Host "  OK: iii-cli downloaded" -ForegroundColor Green
}

# Step 3: Download iii-console (workflow UI)
Write-Host ""
Write-Host "[3/5] Downloading iii-console v0.9.0 (workflow UI)..." -ForegroundColor Yellow
$consoleZip = Join-Path $BIN_DIR "iii-console.zip"
$consoleExe = Join-Path $BIN_DIR "iii-console.exe"

if (Test-Path $consoleExe) {
    Write-Host "  OK: iii-console.exe already exists" -ForegroundColor Green
} else {
    $consoleUrl = "https://github.com/iii-hq/iii/releases/download/iii/v0.9.0/iii-console-x86_64-pc-windows-msvc.zip"
    Write-Host "  Downloading from GitHub..."
    Invoke-WebRequest -Uri $consoleUrl -OutFile $consoleZip -UseBasicParsing
    Expand-Archive -Path $consoleZip -DestinationPath $BIN_DIR -Force
    Remove-Item $consoleZip -ErrorAction SilentlyContinue
    Write-Host "  OK: iii-console downloaded" -ForegroundColor Green
}

# Step 4: Check for uv
Write-Host ""
Write-Host "[4/5] Checking for uv (Python package manager)..." -ForegroundColor Yellow
$uvCheck = Get-Command uv -ErrorAction SilentlyContinue
if ($uvCheck) {
    Write-Host "  OK: uv already installed" -ForegroundColor Green
} else {
    Write-Host "  Installing uv via PowerShell..."
    try {
        Invoke-RestMethod https://astral.sh/uv/install.ps1 | Invoke-Expression
        Write-Host "  OK: uv installed" -ForegroundColor Green
    } catch {
        Write-Host "  WARNING: uv install failed. Install manually: https://astral.sh/uv" -ForegroundColor Red
    }
}

# Step 5: Install Python dependencies
Write-Host ""
Write-Host "[5/5] Installing Python dependencies..." -ForegroundColor Yellow
Push-Location $MOTIA_DIR
try {
    uv sync 2>&1
    Write-Host "  OK: Dependencies installed" -ForegroundColor Green
} catch {
    Write-Host "  WARNING: uv sync failed. Run 'uv sync' manually." -ForegroundColor Yellow
}
Pop-Location

# List bin contents
Write-Host ""
Write-Host "Downloaded binaries:" -ForegroundColor Cyan
Get-ChildItem $BIN_DIR -Filter "*.exe" | ForEach-Object {
    $sizeMB = [math]::Round($_.Length / 1MB, 1)
    Write-Host "  $($_.Name) ($sizeMB MB)" -ForegroundColor White
}

Write-Host ""
Write-Host "=========================================" -ForegroundColor Green
Write-Host "  SETUP COMPLETE!                        " -ForegroundColor Green
Write-Host "=========================================" -ForegroundColor Green
Write-Host ""
Write-Host "To start the workflow engine:" -ForegroundColor White
Write-Host "  .\bin\iii.exe -c iii-config.yaml" -ForegroundColor Cyan
Write-Host ""
Write-Host "To start the workflow UI (new terminal):" -ForegroundColor White
Write-Host "  .\bin\iii-console.exe --enable-flow" -ForegroundColor Cyan
Write-Host ""
Write-Host "Then open: http://localhost:3113/" -ForegroundColor White
Write-Host "API endpoint: http://localhost:3111/query" -ForegroundColor White
Write-Host ""
