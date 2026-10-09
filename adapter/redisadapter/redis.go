package redisadapter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/seu-usuario/onceflow"
)

var (
	ErrRedisAdapter = errors.New("redis adapter error")
)

const acquireScript = `
local key = KEYS[1]
local ttl = ARGV[1]
local now = ARGV[2]
local token = ARGV[3]

local status = redis.call('HGET', key, 'status')

if not status or status == 'FAILED' then
    redis.call('HSET', key, 'status', 'IN_PROGRESS', 'created_at', now, 'lock_token', token)
    redis.call('PEXPIRE', key, ttl)
    return {1}
end

if status == 'IN_PROGRESS' then
    return {0}
end

if status == 'COMPLETED' then
    local statusCode = redis.call('HGET', key, 'status_code')
    local headers = redis.call('HGET', key, 'headers')
    local body = redis.call('HGET', key, 'body')
    local createdAt = redis.call('HGET', key, 'created_at')
    return {2, statusCode, headers, body, createdAt}
end

return {0}
`

const resolveScript = `
local key = KEYS[1]
local ttl = ARGV[1]
local statusCode = ARGV[2]
local headers = ARGV[3]
local body = ARGV[4]
local token = ARGV[5]

local status = redis.call('HGET', key, 'status')
local currentToken = redis.call('HGET', key, 'lock_token')

if status == 'IN_PROGRESS' then
    if currentToken ~= token then
        return -1
    end
    redis.call('HMSET', key, 'status', 'COMPLETED', 'status_code', statusCode, 'headers', headers, 'body', body)
    redis.call('PEXPIRE', key, ttl)
    return 1
end
return 0
`

const failScript = `
local key = KEYS[1]
local token = ARGV[1]

local currentToken = redis.call('HGET', key, 'lock_token')
if currentToken == token then
    redis.call('DEL', key)
    return 1
end
return 0
`

type RedisAdapter struct {
	client *redis.Client
}

func New(client *redis.Client) *RedisAdapter {
	return &RedisAdapter{client: client}
}

func generateToken() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func (r *RedisAdapter) Acquire(ctx context.Context, key string, lockTTL time.Duration) (string, *onceflow.Result, error) {
	now := time.Now().UnixMilli()
	token := generateToken()
	keys := []string{key}
	args := []interface{}{lockTTL.Milliseconds(), now, token}

	res, err := r.client.Eval(ctx, acquireScript, keys, args...).Result()
	if err != nil {
		return "", nil, err
	}

	arr, ok := res.([]interface{})
	if !ok || len(arr) == 0 {
		return "", nil, ErrRedisAdapter
	}

	state, ok := arr[0].(int64)
	if !ok {
		return "", nil, ErrRedisAdapter
	}

	switch state {
	case 1:
		return token, nil, nil
	case 0:
		return "", nil, onceflow.ErrConflict
	case 2:
		if len(arr) < 5 {
			return "", nil, ErrRedisAdapter
		}

		statusCodeStr, _ := arr[1].(string)
		headers, _ := arr[2].(string)
		body, _ := arr[3].(string)
		createdAtStr, _ := arr[4].(string)

		statusCode, _ := strconv.Atoi(statusCodeStr)
		createdAt, _ := strconv.ParseInt(createdAtStr, 10, 64)

		return "", &onceflow.Result{
			Status:     onceflow.StatusCompleted,
			StatusCode: statusCode,
			Headers:    headers,
			Body:       body,
			CreatedAt:  createdAt,
		}, nil
	}

	return "", nil, ErrRedisAdapter
}

func (r *RedisAdapter) Resolve(ctx context.Context, key string, token string, res onceflow.Result, retentionTTL time.Duration) error {
	keys := []string{key}
	args := []interface{}{
		retentionTTL.Milliseconds(),
		strconv.Itoa(res.StatusCode),
		res.Headers,
		res.Body,
		token,
	}

	val, err := r.client.Eval(ctx, resolveScript, keys, args...).Result()
	if err != nil {
		return err
	}
	
	if val.(int64) == -1 {
		return onceflow.ErrTokenMismatch
	}
	return nil
}

func (r *RedisAdapter) Fail(ctx context.Context, key string, token string) error {
	keys := []string{key}
	args := []interface{}{token}
	
	val, err := r.client.Eval(ctx, failScript, keys, args...).Result()
	if err != nil {
		return err
	}
	if val.(int64) == 0 {
		// Either key deleted already, or token mismatched. Both mean lock was lost/expired.
		return onceflow.ErrTokenMismatch
	}
	return nil
}
