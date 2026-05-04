package bootstrap

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

type otelHandler struct {
	slog.Handler
}

func NewLogger(h slog.Handler) *slog.Logger {
	return slog.New(otelHandler{Handler: h})
}

func (h otelHandler) Handle(ctx context.Context, r slog.Record) error {
	if spanCtx := trace.SpanContextFromContext(ctx); spanCtx.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", spanCtx.TraceID().String()),
			slog.String("span_id", spanCtx.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, r)
}