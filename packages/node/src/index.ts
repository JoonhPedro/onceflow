export type Status = 'IN_PROGRESS' | 'COMPLETED' | 'FAILED';

export interface Result {
    status: Status;
    statusCode: number;
    headers: string;
    body: string;
    createdAt: number;
}

export interface Storage {
    acquire(key: string, lockTTL: number): Promise<Result | null>;
    resolve(key: string, res: Result, retentionTTL: number): Promise<void>;
    fail(key: string): Promise<void>;
}

export class Engine {
    constructor(
        private storage: Storage,
        private lockTTL: number = 30000,
        private retentionTTL: number = 86400000
    ) {}

    async acquire(key: string): Promise<Result | null> {
        return this.storage.acquire(key, this.lockTTL);
    }

    async resolve(key: string, res: Result): Promise<void> {
        return this.storage.resolve(key, res, this.retentionTTL);
    }

    async fail(key: string): Promise<void> {
        return this.storage.fail(key);
    }
}
