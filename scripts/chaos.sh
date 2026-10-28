#!/usr/bin/env bash
# scripts/chaos.sh - Chaos Monkey Local para Cargo-Slots
set -e

echo "=== [CHAOS MONKEY] Iniciando inyección de fallos en infraestructura ==="

# 1. Iniciar tráfico k6 en background
echo "[CHAOS] Lanzando generador de carga con k6 en background..."
k6 run scripts/load_test_chaos.js &
K6_PID=$!

sleep 1

# 2. Detener DB durante 2 segundos
echo "[CHAOS]>>> STOPPING POSTGRESQL (docker compose stop db)..."
docker compose stop db
sleep 2

# 3. Arrancar DB (Auto-Healing)
echo "[CHAOS]>>> STARTING POSTGRESQL (docker compose start db)..."
docker compose start db
sleep 2

# 4. Detener Redis durante 1 segundo
echo "[CHAOS]>>> STOPPING REDIS (docker compose stop redis)..."
docker compose stop redis
sleep 1

# 5. Arrancar Redis (Auto-Healing)
echo "[CHAOS]>>> STARTING REDIS (docker compose start redis)..."
docker compose start redis

wait $K6_PID
echo "=== [CHAOS MONKEY] Ciclo de caos finalizado exitosamente ==="
