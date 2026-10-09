package onceflow

import "errors"

type Status string

const (
	StatusInProgress Status = "IN_PROGRESS"
	StatusCompleted  Status = "COMPLETED"
	StatusFailed     Status = "FAILED"
)

var (
	ErrConflict      = errors.New("conflict: operation in progress")
	ErrTokenMismatch = errors.New("token mismatch: lock is held by another process")
)

type Result struct {
	Status     Status
	StatusCode int
	Headers    string
	Body       string
	CreatedAt  int64
}
