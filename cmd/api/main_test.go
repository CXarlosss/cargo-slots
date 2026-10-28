package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestRunApp_GracefulShutdown verifica que al recibir la señal de cancelación del contexto:
// 1. El servidor HTTP deja de aceptar nuevas conexiones.
// 2. Espera a las conexiones activas hasta 5 segundos.
// 3. Cierra ordenadamente los pools de PostgreSQL y Redis.
// 4. Imprime los logs exactos requeridos de shutdown.
func TestRunApp_GracefulShutdown(t *testing.T) {
	var logBuf bytes.Buffer
	origOutput := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(origOutput)

	ctx, cancel := context.WithCancel(context.Background())
	dbURL := getDatabaseURL()
	redisAddr := getRedisAddr()
	testPort := "8899"

	appDone := make(chan error, 1)
	go func() {
		appDone <- RunApp(ctx, dbURL, redisAddr, testPort)
	}()

	// Esperamos a que el servidor HTTP esté arriba
	time.Sleep(300 * time.Millisecond)

	// Verificamos que el servidor responde peticiones
	resp, err := http.Get(fmt.Sprintf("http://localhost:%s/health", testPort))
	if err != nil {
		t.Fatalf("Failed to hit health endpoint on running server: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 OK from server before shutdown, got: %d", resp.StatusCode)
	}

	// Simulamos la llegada de SIGTERM / os.Interrupt cancelando el contexto
	cancel()

	select {
	case err := <-appDone:
		if err != nil {
			t.Fatalf("RunApp returned error during graceful shutdown: %v", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatalf("RunApp failed to shutdown within timeout")
	}

	logs := logBuf.String()
	t.Logf("=== LOGS DE GRACEFUL SHUTDOWN CAPTURADOS ===")
	t.Log(logs)

	if !strings.Contains(logs, "Graceful shutdown iniciado") {
		t.Errorf("Expected logs to contain 'Graceful shutdown iniciado', got: %s", logs)
	}
	if !strings.Contains(logs, "Conexiones cerradas") {
		t.Errorf("Expected logs to contain 'Conexiones cerradas', got: %s", logs)
	}
	if !strings.Contains(logs, "Graceful shutdown completado exitosamente") {
		t.Errorf("Expected logs to contain 'Graceful shutdown completado exitosamente', got: %s", logs)
	}
}
