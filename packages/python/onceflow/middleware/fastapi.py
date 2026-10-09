import json
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from ..core import Engine, Result, ConflictError

class OnceFlowMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, engine: Engine, header_key: str = "idempotency-key", namespace: str = "default"):
        super().__init__(app)
        self.engine = engine
        self.header_key = header_key.lower()
        self.namespace = namespace

    async def dispatch(self, request: Request, call_next):
        idempotency_key = request.headers.get(self.header_key)
        if not idempotency_key:
            return await call_next(request)
        
        full_key = f"onceflow:{self.namespace}:{idempotency_key}"
        
        try:
            cached = self.engine.acquire(full_key)
            if cached:
                headers = {}
                if cached.headers:
                    try:
                        headers = json.loads(cached.headers)
                    except:
                        pass
                return Response(content=cached.body, status_code=cached.status_code, headers=headers)
        except ConflictError:
            return Response(content=json.dumps({"error": "conflict", "message": "operation in progress"}), status_code=409, media_type="application/json")
        except Exception as e:
            return Response(content=json.dumps({"error": "internal", "message": "idempotency engine error"}), status_code=500, media_type="application/json")

        response = await call_next(request)
        
        if response.status_code >= 500:
            self.engine.fail(full_key)
        else:
            body = b""
            async for chunk in response.body_iterator:
                body += chunk
                
            headers_dict = dict(response.headers)
            headers_json = json.dumps(headers_dict)
            
            res = Result(
                status="COMPLETED",
                status_code=response.status_code,
                headers=headers_json,
                body=body.decode('utf-8'),
                created_at=0
            )
            self.engine.resolve(full_key, res)
            
            return Response(content=body, status_code=response.status_code, headers=headers_dict)
        
        return response
