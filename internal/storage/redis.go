package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"cargo-slots/internal/domain"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// releaseLockScript comprueba atómicamente si el valor asociado a la clave coincide con el token
// del poseedor del lock antes de eliminarlo. Si coincide, borra la clave y retorna 1; de lo contrario retorna 0.
var releaseLockScript = redis.NewScript(`
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
    else
        return 0
    end
`)

type RedisLocker struct {
	client redis.Cmdable
}

// NewRedisClient crea y verifica una conexión estándar con Redis usando go-redis/v9.
func NewRedisClient(ctx context.Context, addr, password string, db int) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to ping redis at %s: %w", addr, err)
	}

	return client, nil
}

// NewRedisLocker inicializa el locker distribuido.
func NewRedisLocker(client redis.Cmdable) *RedisLocker {
	return &RedisLocker{
		client: client,
	}
}

// Acquire intenta obtener el lock exclusivo en Redis para el recurso key durante ttlMs milisegundos.
// Devuelve el token (UUID) en caso de éxito o domain.ErrLockBusy si el slot ya está bloqueado.
func (l *RedisLocker) Acquire(ctx context.Context, key string, ttlMs int) (string, error) {
	token := uuid.New().String()

	// SET key token NX PX ttlMs
	success, err := l.client.SetNX(ctx, key, token, time.Duration(ttlMs)*time.Millisecond).Result()
	if err != nil {
		return "", fmt.Errorf("redis SetNX error: %w", err)
	}
	if !success {
		return "", domain.ErrLockBusy
	}

	return token, nil
}

// Release ejecuta el script Lua atómico para liberar el lock solo si el token coincide.
// Evita borrar locks ajenos en caso de demoras o expiración del TTL.
func (l *RedisLocker) Release(ctx context.Context, key, token string) error {
	res, err := releaseLockScript.Run(ctx, l.client, []string{key}, token).Result()
	if err != nil {
		return fmt.Errorf("redis release lock script execution error: %w", err)
	}

	deleted, ok := res.(int64)
	if !ok || deleted == 0 {
		log.Printf("[WARN] Attempted to release lock %s with token %s, but lock was expired or owned by another process", key, token)
		return domain.ErrLockNotOwned
	}

	return nil
}

// Publish serializa el mensaje a JSON y lo publica en el canal Redis especificado.
func (l *RedisLocker) Publish(ctx context.Context, channel string, message any) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message for pubsub: %w", err)
	}
	return l.client.Publish(ctx, channel, payload).Err()
}

// Ping comprueba la conectividad con Redis.
func (l *RedisLocker) Ping(ctx context.Context) error {
	return l.client.Ping(ctx).Err()
}
