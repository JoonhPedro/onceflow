package onceflow

import (
	"context"
	"time"
)

type Config struct {
	Storage      Storage
	LockTTL      time.Duration
	RetentionTTL time.Duration
}

type Engine struct {
	config Config
}

func New(cfg Config) *Engine {
	if cfg.LockTTL == 0 {
		cfg.LockTTL = 30 * time.Second
	}
	if cfg.RetentionTTL == 0 {
		cfg.RetentionTTL = 24 * time.Hour
	}
	return &Engine{config: cfg}
}

func (e *Engine) Acquire(ctx context.Context, key string) (string, *Result, error) {
	return e.config.Storage.Acquire(ctx, key, e.config.LockTTL)
}

func (e *Engine) Resolve(ctx context.Context, key string, token string, res Result) error {
	return e.config.Storage.Resolve(ctx, key, token, res, e.config.RetentionTTL)
}

func (e *Engine) Fail(ctx context.Context, key string, token string) error {
	return e.config.Storage.Fail(ctx, key, token)
}
