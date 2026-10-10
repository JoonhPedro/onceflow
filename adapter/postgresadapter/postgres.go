package postgresadapter

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"

	"github.com/JoonhPedro/onceflow"
)

// SQL schema needed in your database:
/*
CREATE TABLE IF NOT EXISTS onceflow_keys (
    key VARCHAR(255) PRIMARY KEY,
    status VARCHAR(50) NOT NULL,
    status_code INT,
    headers TEXT,
    body TEXT,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    expires_at BIGINT NOT NULL,
    lock_token VARCHAR(255)
);
*/

type PostgresAdapter struct {
	db *sql.DB
}

func New(db *sql.DB) *PostgresAdapter {
	return &PostgresAdapter{db: db}
}

func generateToken() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func (p *PostgresAdapter) Acquire(ctx context.Context, key string, lockTTL time.Duration) (string, *onceflow.Result, error) {
	now := time.Now().UnixMilli()
	expiresAt := time.Now().Add(lockTTL).UnixMilli()
	token := generateToken()

	// Try to insert atomically
	// If it succeeds, we got the lock.
	res, err := p.db.ExecContext(ctx, `
		INSERT INTO onceflow_keys (key, status, created_at, updated_at, expires_at, lock_token)
		VALUES ($1, 'IN_PROGRESS', $2, $3, $4, $5)
		ON CONFLICT (key) DO NOTHING
	`, key, now, now, expiresAt, token)
	
	if err != nil {
		return "", nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return "", nil, err
	}

	if rowsAffected > 0 {
		return token, nil, nil // Acquired lock successfully
	}

	// Key exists, let's fetch its state
	var status string
	var statusCode sql.NullInt32
	var headers, body sql.NullString
	var createdAt, updatedAt, currentExpiresAt int64

	err = p.db.QueryRowContext(ctx, `
		SELECT status, status_code, headers, body, created_at, updated_at, expires_at
		FROM onceflow_keys WHERE key = $1
	`, key).Scan(&status, &statusCode, &headers, &body, &createdAt, &updatedAt, &currentExpiresAt)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Extremely rare race condition where key was deleted between insert and select
			return p.Acquire(ctx, key, lockTTL) 
		}
		return "", nil, err
	}

	if status == string(onceflow.StatusCompleted) {
		if now > currentExpiresAt {
			// RetentionTTL expired. Take over as a new request.
			updateRes, _ := p.db.ExecContext(ctx, `
				UPDATE onceflow_keys 
				SET status = 'IN_PROGRESS', created_at = $1, updated_at = $2, expires_at = $3, lock_token = $4
				WHERE key = $5 AND expires_at = $6
			`, now, now, expiresAt, token, key, currentExpiresAt)
			
			affected, _ := updateRes.RowsAffected()
			if affected > 0 {
				return token, nil, nil // Successfully took over the expired cached result
			}
			// Someone else took over exactly at this moment, return conflict
			return "", nil, onceflow.ErrConflict
		}

		return "", &onceflow.Result{
			Status:     onceflow.StatusCompleted,
			StatusCode: int(statusCode.Int32),
			Headers:    headers.String,
			Body:       body.String,
			CreatedAt:  createdAt,
		}, nil
	}

	if status == string(onceflow.StatusInProgress) {
		if now > currentExpiresAt {
			// Lock expired (previous process died/crashed). Take over.
			updateRes, _ := p.db.ExecContext(ctx, `
				UPDATE onceflow_keys 
				SET status = 'IN_PROGRESS', created_at = $1, updated_at = $2, expires_at = $3, lock_token = $4
				WHERE key = $5 AND expires_at = $6
			`, now, now, expiresAt, token, key, currentExpiresAt)
			
			affected, _ := updateRes.RowsAffected()
			if affected > 0 {
				return token, nil, nil // Successfully took over the expired lock
			}
			// Someone else took over exactly at this moment, return conflict
		}
		return "", nil, onceflow.ErrConflict
	}

	if status == string(onceflow.StatusFailed) {
		// Reset state
		updateRes, err := p.db.ExecContext(ctx, `
			UPDATE onceflow_keys 
			SET status = 'IN_PROGRESS', created_at = $1, updated_at = $2, expires_at = $3, lock_token = $4
			WHERE key = $5 AND status = 'FAILED'
		`, now, now, expiresAt, token, key)
		if err == nil {
			affected, _ := updateRes.RowsAffected()
			if affected > 0 {
				return token, nil, nil
			}
		}
	}

	return "", nil, onceflow.ErrConflict
}

func (p *PostgresAdapter) Resolve(ctx context.Context, key string, token string, res onceflow.Result, retentionTTL time.Duration) error {
	now := time.Now().UnixMilli()
	expiresAt := time.Now().Add(retentionTTL).UnixMilli()

	result, err := p.db.ExecContext(ctx, `
		UPDATE onceflow_keys 
		SET status = 'COMPLETED', status_code = $1, headers = $2, body = $3, updated_at = $4, expires_at = $5
		WHERE key = $6 AND status = 'IN_PROGRESS' AND lock_token = $7
	`, res.StatusCode, res.Headers, res.Body, now, expiresAt, key, token)
	
	if err != nil {
		return err
	}
	
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return onceflow.ErrTokenMismatch
	}

	return nil
}

func (p *PostgresAdapter) Fail(ctx context.Context, key string, token string) error {
	result, err := p.db.ExecContext(ctx, `DELETE FROM onceflow_keys WHERE key = $1 AND lock_token = $2`, key, token)
	if err != nil {
		return err
	}
	
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return onceflow.ErrTokenMismatch
	}
	return nil
}
