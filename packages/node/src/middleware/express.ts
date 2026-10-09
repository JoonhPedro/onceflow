import { Request, Response, NextFunction } from 'express';
import { Engine } from '../index';

export interface OnceFlowOptions {
    headerKey?: string;
    namespace?: string;
}

export function onceflowMiddleware(engine: Engine, options: OnceFlowOptions = {}) {
    const headerKey = (options.headerKey || 'idempotency-key').toLowerCase();
    const namespace = options.namespace || 'default';

    return async (req: Request, res: Response, next: NextFunction) => {
        const idempotencyKey = req.headers[headerKey];
        if (!idempotencyKey || typeof idempotencyKey !== 'string') {
            return next();
        }

        const fullKey = `onceflow:${namespace}:${idempotencyKey}`;

        try {
            const cachedRes = await engine.acquire(fullKey);
            if (cachedRes) {
                if (cachedRes.headers) {
                    try {
                        const parsedHeaders = JSON.parse(cachedRes.headers);
                        for (const [k, v] of Object.entries(parsedHeaders)) {
                            res.setHeader(k, v as string | string[]);
                        }
                    } catch (e) {}
                }
                return res.status(cachedRes.statusCode).send(cachedRes.body);
            }
        } catch (err: any) {
            if (err.message === 'conflict: operation in progress') {
                return res.status(409).json({ error: 'conflict', message: 'operation in progress' });
            }
            return res.status(500).json({ error: 'internal', message: 'idempotency engine error' });
        }

        const originalSend = res.send.bind(res);
        let responseCaptured = false;

        res.send = (body: any) => {
            if (!responseCaptured) {
                responseCaptured = true;
                const statusCode = res.statusCode;
                
                if (statusCode >= 500) {
                    engine.fail(fullKey).catch(console.error);
                } else {
                    engine.resolve(fullKey, {
                        status: 'COMPLETED',
                        statusCode,
                        headers: JSON.stringify(res.getHeaders()),
                        body: typeof body === 'string' ? body : JSON.stringify(body),
                        createdAt: Date.now()
                    }).catch(console.error);
                }
            }
            return originalSend(body);
        };

        next();
    };
}
