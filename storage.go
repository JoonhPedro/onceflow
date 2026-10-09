package onceflow

import (
	"context"
	"time"
)

type Storage interface {
	Acquire(ctx context.Context, key string, lockTTL time.Duration) (string, *Result, error)
	Resolve(ctx context.Context, key string, token string, res Result, retentionTTL time.Duration) error
	Fail(ctx context.Context, key string, token string) error
}
