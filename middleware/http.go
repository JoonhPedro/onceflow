package middleware

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"github.com/JoonhPedro/onceflow"
)

type Options struct {
	HeaderKey string
	Namespace string
}

type responseRecorder struct {
	http.ResponseWriter
	statusCode int
	body       bytes.Buffer
}

func (r *responseRecorder) WriteHeader(statusCode int) {
	r.statusCode = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	r.body.Write(b)
	return r.ResponseWriter.Write(b)
}

func Wrap(engine *onceflow.Engine, next http.Handler, opts Options) http.Handler {
	if opts.HeaderKey == "" {
		opts.HeaderKey = "Idempotency-Key"
	}
	if opts.Namespace == "" {
		opts.Namespace = "default"
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idempotencyKey := r.Header.Get(opts.HeaderKey)
		if idempotencyKey == "" {
			next.ServeHTTP(w, r)
			return
		}

		fullKey := "onceflow:" + opts.Namespace + ":" + idempotencyKey

		ctx := r.Context()
		token, res, err := engine.Acquire(ctx, fullKey)

		if err != nil {
			if err == onceflow.ErrConflict {
				http.Error(w, `{"error":"conflict","message":"operation in progress"}`, http.StatusConflict)
				return
			}
			http.Error(w, `{"error":"internal","message":"idempotency engine error"}`, http.StatusInternalServerError)
			return
		}

		if res != nil {
			if res.Headers != "" {
				var headers map[string][]string
				if err := json.Unmarshal([]byte(res.Headers), &headers); err == nil {
					for k, v := range headers {
						for _, vv := range v {
							w.Header().Add(k, vv)
						}
					}
				}
			}
			w.WriteHeader(res.StatusCode)
			w.Write([]byte(res.Body))
			return
		}

		rec := &responseRecorder{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		defer func() {
			if p := recover(); p != nil {
				engine.Fail(context.Background(), fullKey, token)
				panic(p)
			}

			if rec.statusCode >= 500 {
				engine.Fail(context.Background(), fullKey, token)
				return
			}

			// Se o request foi cancelado/abortado mas o status ainda era 200, podemos tratar como falha pra não salvar corpo vazio
			if r.Context().Err() != nil {
				engine.Fail(context.Background(), fullKey, token)
				return
			}

			headersJSON, _ := json.Marshal(rec.Header())
			
			result := onceflow.Result{
				StatusCode: rec.statusCode,
				Headers:    string(headersJSON),
				Body:       rec.body.String(),
			}

			engine.Resolve(context.Background(), fullKey, token, result)
		}()

		next.ServeHTTP(rec, r)
	})
}
