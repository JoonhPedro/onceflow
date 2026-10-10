import time
import uuid
from typing import Optional, Tuple
from redis import Redis
from .core import Storage, Result, ConflictError

ACQUIRE_SCRIPT = """
local key = KEYS[1]
local ttl = ARGV[1]
local now = ARGV[2]
local token = ARGV[3]

local status = redis.call('HGET', key, 'status')

if not status or status == 'FAILED' then
    redis.call('HSET', key, 'status', 'IN_PROGRESS', 'created_at', now, 'token', token)
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
"""

RESOLVE_SCRIPT = """
local key = KEYS[1]
local ttl = ARGV[1]
local statusCode = ARGV[2]
local headers = ARGV[3]
local body = ARGV[4]
local token = ARGV[5]

local status = redis.call('HGET', key, 'status')
local currentToken = redis.call('HGET', key, 'token')

if status == 'IN_PROGRESS' and currentToken == token then
    redis.call('HMSET', key, 'status', 'COMPLETED', 'status_code', statusCode, 'headers', headers, 'body', body)
    redis.call('PEXPIRE', key, ttl)
    return 1
end
return 0
"""

FAIL_SCRIPT = """
local key = KEYS[1]
local token = ARGV[1]

local status = redis.call('HGET', key, 'status')
local currentToken = redis.call('HGET', key, 'token')

if status == 'IN_PROGRESS' and currentToken == token then
    redis.call('DEL', key)
    return 1
end
return 0
"""

class RedisAdapter(Storage):
    def __init__(self, client: Redis):
        self.client = client
        self._acquire = client.register_script(ACQUIRE_SCRIPT)
        self._resolve = client.register_script(RESOLVE_SCRIPT)
        self._fail = client.register_script(FAIL_SCRIPT)

    def acquire(self, key: str, lock_ttl: int) -> Tuple[Optional[str], Optional[Result]]:
        now = int(time.time() * 1000)
        token = str(uuid.uuid4())
        res = self._acquire(keys=[key], args=[lock_ttl, now, token])
        
        state = res[0]
        if state == 1:
            return token, None
        if state == 0:
            raise ConflictError("conflict: operation in progress")
        if state == 2:
            return None, Result(
                status="COMPLETED",
                status_code=int(res[1]),
                headers=res[2].decode('utf-8') if isinstance(res[2], bytes) else res[2],
                body=res[3].decode('utf-8') if isinstance(res[3], bytes) else res[3],
                created_at=int(res[4])
            )
        raise Exception("Redis adapter error")

    def resolve(self, key: str, token: str, res: Result, retention_ttl: int) -> None:
        self._resolve(keys=[key], args=[
            retention_ttl,
            res.status_code,
            res.headers,
            res.body,
            token
        ])

    def fail(self, key: str, token: str) -> None:
        self._fail(keys=[key], args=[token])
