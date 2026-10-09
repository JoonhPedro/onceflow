import { Redis } from 'ioredis';
import { Result, Storage } from './index';

const ACQUIRE_SCRIPT = `
local key = KEYS[1]
local ttl = ARGV[1]
local now = ARGV[2]

local status = redis.call('HGET', key, 'status')

if not status or status == 'FAILED' then
    redis.call('HSET', key, 'status', 'IN_PROGRESS', 'created_at', now)
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
`;

const RESOLVE_SCRIPT = `
local key = KEYS[1]
local ttl = ARGV[1]
local statusCode = ARGV[2]
local headers = ARGV[3]
local body = ARGV[4]

local status = redis.call('HGET', key, 'status')
if status == 'IN_PROGRESS' then
    redis.call('HMSET', key, 'status', 'COMPLETED', 'status_code', statusCode, 'headers', headers, 'body', body)
    redis.call('PEXPIRE', key, ttl)
    return 1
end
return 0
`;

export class RedisAdapter implements Storage {
    constructor(private client: Redis) {}

    async acquire(key: string, lockTTL: number): Promise<Result | null> {
        const now = Date.now();
        const res = await this.client.eval(ACQUIRE_SCRIPT, 1, key, lockTTL, now) as any[];
        
        if (!res || res.length === 0) throw new Error('Redis adapter error');
        
        const state = res[0];
        
        if (state === 1) return null;
        if (state === 0) throw new Error('conflict: operation in progress');
        if (state === 2) {
            return {
                status: 'COMPLETED',
                statusCode: parseInt(res[1], 10),
                headers: res[2],
                body: res[3],
                createdAt: parseInt(res[4], 10)
            };
        }
        
        throw new Error('Redis adapter error');
    }

    async resolve(key: string, res: Result, retentionTTL: number): Promise<void> {
        await this.client.eval(
            RESOLVE_SCRIPT, 1, key,
            retentionTTL,
            res.statusCode.toString(),
            res.headers,
            res.body
        );
    }

    async fail(key: string): Promise<void> {
        await this.client.del(key);
    }
}
