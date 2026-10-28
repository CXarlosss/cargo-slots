package domain

import "context"

// DockEvent representa el payload transmitido en tiempo real vía Redis Pub/Sub y SSE.
type DockEvent struct {
	DockID      string `json:"dock_id"`
	DockName    string `json:"dock_name,omitempty"`
	ForwarderID string `json:"forwarder_id,omitempty"`
	Status      string `json:"status"`
	TimeRange   string `json:"time_range"`
	Timestamp   string `json:"timestamp"`
}

// EventPublisher define el contrato para publicar eventos en tiempo real.
type EventPublisher interface {
	Publish(ctx context.Context, channel string, message any) error
}
