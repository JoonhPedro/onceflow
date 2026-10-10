package onceflow_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JoonhPedro/onceflow"
	"github.com/JoonhPedro/onceflow/middleware"
	"github.com/JoonhPedro/onceflow/mock"
)

func TestConcurrencyIdempotency(t *testing.T) {
	memStorage := mock.NewMemoryStorage()
	engine := onceflow.New(onceflow.Config{
		Storage:      memStorage,
		LockTTL:      5 * time.Second,
		RetentionTTL: 1 * time.Hour,
	})

	var executionCounter int32

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&executionCounter, 1)
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"success":true}`))
	})

	wrappedHandler := middleware.Wrap(engine, handler, middleware.Options{
		HeaderKey: "Idempotency-Key",
	})

	concurrencyLevel := 10
	var wg sync.WaitGroup
	wg.Add(concurrencyLevel)

	for i := 0; i < concurrencyLevel; i++ {
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/charge", nil)
			req.Header.Set("Idempotency-Key", "payment_attempt_123")
			rec := httptest.NewRecorder()

			wrappedHandler.ServeHTTP(rec, req)
		}()
	}

	wg.Wait()

	if atomic.LoadInt32(&executionCounter) != 1 {
		t.Fatalf("esperado exatamente 1 execucao real, obtido: %d", atomic.LoadInt32(&executionCounter))
	}
}

func TestEngine_AcquireAndResolve(t *testing.T) {
	memStorage := mock.NewMemoryStorage()
	engine := onceflow.New(onceflow.Config{
		Storage:      memStorage,
		LockTTL:      1 * time.Second,
		RetentionTTL: 1 * time.Minute,
	})
	ctx := context.Background()
	key := "onceflow:test:key1"

	token, res, err := engine.Acquire(ctx, key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != nil {
		t.Fatalf("expected nil result on first acquire")
	}
	if token == "" {
		t.Fatalf("expected valid lock token")
	}

	_, res, err = engine.Acquire(ctx, key)
	if err != onceflow.ErrConflict {
		t.Fatalf("expected conflict error, got %v", err)
	}

	result := onceflow.Result{
		StatusCode: 200,
		Body:       "ok",
	}
	err = engine.Resolve(ctx, key, token, result)
	if err != nil {
		t.Fatalf("unexpected error resolving: %v", err)
	}

	_, res, err = engine.Acquire(ctx, key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res == nil {
		t.Fatalf("expected result, got nil")
	}
	if res.StatusCode != 200 {
		t.Fatalf("expected status 200, got %d", res.StatusCode)
	}
}
