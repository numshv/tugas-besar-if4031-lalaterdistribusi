package publisher

import (
	"context"
	"log/slog"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
)

// Dipakai saat development lokal sebelum Kafka tersedia, atau di unit test.
type LoggingPublisher struct{ Log *slog.Logger }

func (p LoggingPublisher) Publish(ctx context.Context, e hazard.Event) error {
	p.Log.InfoContext(ctx, "would publish (dev mode)", "hazard_id", e.HazardID, "severity", e.Severity)
	return nil
}
func (p LoggingPublisher) Close() error { return nil }
