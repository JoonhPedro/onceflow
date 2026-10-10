export type Status = 'IN_PROGRESS' | 'COMPLETED' | 'FAILED';

export interface Result {
    status: Status;
    statusCode: number;
    headers: string;
    body: string;
    createdAt: number;
}

export interface Storage {
    acquire(key: string, lockTTL: number): Promise<[string | null, Result | null]>;
    resolve(key: string, token: string, res: Result, retentionTTL: number): Promise<void>;
    fail(key: string, token: string): Promise<void>;
}

export class Engine {
    constructor(
        private storage: Storage,
        private lockTTL: number = 30000,
        private retentionTTL: number = 86400000
    ) {}

    async acquire(key: string): Promise<[string | null, Result | null]> {
        return this.storage.acquire(key, this.lockTTL);
    }

    async resolve(key: string, token: string, res: Result): Promise<void> {
        return this.storage.resolve(key, token, res, this.retentionTTL);
    }

    async fail(key: string, token: string): Promise<void> {
        return this.storage.fail(key, token);
    }
}
