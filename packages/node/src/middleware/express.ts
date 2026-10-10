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

        let token: string | null = null;
        try {
            const [acqToken, cachedRes] = await engine.acquire(fullKey);
            token = acqToken;
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

        res.send = (body?: any) => {
            if (!responseCaptured && token) {
                responseCaptured = true;
                const statusCode = res.statusCode;
                
                if (statusCode >= 500) {
                    engine.fail(fullKey, token).catch(console.error);
                } else {
                    engine.resolve(fullKey, token, {
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

        res.on('finish', () => {
            if (!responseCaptured && token) {
                responseCaptured = true;
                if (res.statusCode >= 500) {
                    engine.fail(fullKey, token).catch(console.error);
                } else {
                    engine.resolve(fullKey, token, {
                        status: 'COMPLETED',
                        statusCode: res.statusCode,
                        headers: JSON.stringify(res.getHeaders()),
                        body: '',
                        createdAt: Date.now()
                    }).catch(console.error);
                }
            }
        });

        res.on('close', () => {
            if (!responseCaptured && token) {
                responseCaptured = true;
                engine.fail(fullKey, token).catch(console.error);
            }
        });

        next();
    };
}
