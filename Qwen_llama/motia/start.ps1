Write-Host "Cleaning up previous Motia processes to free up ports..."
$procs = Get-Process -Name "iii", "iii-console", "uv", "python" -ErrorAction SilentlyContinue
foreach ($p in $procs) {
    try {
        if ($p.Name -like "iii*") {
            Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
        } elseif ($p.Path -match "motia\\\.venv") {
            Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
        }
    } catch {}
}
Start-Sleep -Seconds 1

Write-Host "--------------------------------------------------------"
Write-Host "🚀 Starting Motia Engine... (Press Ctrl+C to stop)"
Write-Host "--------------------------------------------------------"

try {
    # This runs the engine in the foreground.
    # When you press Ctrl+C, PowerShell will terminate it and move to the 'finally' block!
    .\bin\iii.exe -c iii-config.yaml
} finally {
    Write-Host "`n--------------------------------------------------------"
    Write-Host "🛑 Terminating all orphaned background workers..."
    Write-Host "--------------------------------------------------------"
    
    $procs = Get-Process -Name "iii", "iii-console", "uv", "python" -ErrorAction SilentlyContinue
    foreach ($p in $procs) {
        try {
            if ($p.Name -like "iii*") {
                Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
            } elseif ($p.Path -match "motia\\\.venv") {
                Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
            }
        } catch {}
    }
    Write-Host "✅ All Motia processes fully terminated."
}
