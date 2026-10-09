# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - Initial Release

### Added
- Core `Onceflow` engine for atomic idempotency locking.
- **Node.js** adapter for Express and standard HTTP servers (`@onceflow/core`).
- **Python** adapter with async support for FastAPI and Starlette.
- **Go** module with strict typing and zero-allocation philosophy.
- **Redis Adapter**: High-performance distributed locking using Lua scripts.
- **PostgreSQL Adapter**: Relational database support via `INSERT ... ON CONFLICT DO NOTHING`.
- **Memory Adapter**: `sync.Map` / dictionary implementation for local testing and unit tests.
- Comprehensive multilanguage documentation (English, Spanish, Russian, Portuguese) built with VitePress.
