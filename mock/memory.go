package mock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/seu-usuario/onceflow"
)

type memoryEntry struct {
	Result    onceflow.Result
	ExpiresAt time.Time
	LockToken string
}

type MemoryStorage struct {
	mu   sync.Mutex
	data map[string]*memoryEntry
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		data: make(map[string]*memoryEntry),
	}
}

func generateToken() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func (m *MemoryStorage) Acquire(ctx context.Context, key string, lockTTL time.Duration) (string, *onceflow.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	entry, exists := m.data[key]

	if exists && now.After(entry.ExpiresAt) {
		delete(m.data, key)
		exists = false
	}

	token := generateToken()

	if !exists || entry.Result.Status == onceflow.StatusFailed {
		m.data[key] = &memoryEntry{
			Result: onceflow.Result{
				Status:    onceflow.StatusInProgress,
				CreatedAt: now.UnixMilli(),
			},
			ExpiresAt: now.Add(lockTTL),
			LockToken: token,
		}
		return token, nil, nil
	}

	if entry.Result.Status == onceflow.StatusInProgress {
		return "", nil, onceflow.ErrConflict
	}

	if entry.Result.Status == onceflow.StatusCompleted {
		res := entry.Result
		return "", &res, nil
	}

	return "", nil, onceflow.ErrConflict
}

func (m *MemoryStorage) Resolve(ctx context.Context, key string, token string, res onceflow.Result, retentionTTL time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, exists := m.data[key]
	if !exists || entry.Result.Status != onceflow.StatusInProgress || entry.LockToken != token {
		return onceflow.ErrTokenMismatch
	}

	res.Status = onceflow.StatusCompleted
	m.data[key] = &memoryEntry{
		Result:    res,
		ExpiresAt: time.Now().Add(retentionTTL),
		LockToken: token,
	}
	return nil
}

func (m *MemoryStorage) Fail(ctx context.Context, key string, token string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, exists := m.data[key]
	if !exists || entry.LockToken != token {
		return onceflow.ErrTokenMismatch
	}
	delete(m.data, key)
	return nil
}
