# scripts/chaos.ps1 - Chaos Monkey Local para Cargo-Slots (PowerShell)
Write-Host "=== [CHAOS MONKEY] Iniciando inyeccion de fallos en infraestructura ===" -ForegroundColor Yellow

$k6Path = "C:\Program Files\k6\k6.exe"
if (-not (Test-Path $k6Path)) {
    $k6Cmd = Get-Command k6 -ErrorAction SilentlyContinue
    if ($k6Cmd) { $k6Path = $k6Cmd.Source } else { $k6Path = "k6" }
}

# 1. Iniciar tráfico con k6 en background
Write-Host "[CHAOS] Lanzando generador de carga con k6..." -ForegroundColor Cyan
$k6Process = Start-Process -FilePath $k6Path -ArgumentList "run scripts/load_test_chaos.js" -PassThru

Start-Sleep -Seconds 1

# 2. Inyección de caos: Stop DB por 2 segundos
Write-Host "[CHAOS]>>> STOPPING POSTGRESQL (docker compose stop db)..." -ForegroundColor Red
docker compose stop db
Write-Host "[CHAOS]>>> PostgreSQL DETENIDO. Esperando 2 segundos..." -ForegroundColor Red
Start-Sleep -Seconds 2

# 3. Recuperación de DB (Auto-Healing)
Write-Host "[CHAOS]>>> STARTING POSTGRESQL (docker compose start db)..." -ForegroundColor Green
docker compose start db
Write-Host "[CHAOS]>>> PostgreSQL REINICIADO (Auto-Healing en progreso)..." -ForegroundColor Green
Start-Sleep -Seconds 2

# 4. Inyección de caos: Stop Redis por 1 segundo
Write-Host "[CHAOS]>>> STOPPING REDIS (docker compose stop redis)..." -ForegroundColor Red
docker compose stop redis
Write-Host "[CHAOS]>>> Redis DETENIDO. Esperando 1 segundo..." -ForegroundColor Red
Start-Sleep -Seconds 1

# 5. Recuperación de Redis (Auto-Healing)
Write-Host "[CHAOS]>>> STARTING REDIS (docker compose start redis)..." -ForegroundColor Green
docker compose start redis
Write-Host "[CHAOS]>>> Redis REINICIADO (Auto-Healing en progreso)..." -ForegroundColor Green

# Esperar a que el proceso de k6 finalice
if ($k6Process -and -not $k6Process.HasExited) {
    Write-Host "[CHAOS] Esperando a que k6 finalice el ciclo de carga..." -ForegroundColor Cyan
    $k6Process.WaitForExit()
}

Write-Host "=== [CHAOS MONKEY] Ciclo de caos finalizado exitosamente ===" -ForegroundColor Yellow
