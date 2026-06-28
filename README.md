# SurgeFlow — Low-Latency High-Concurrency Transactional Ledger

> Java 21 Virtual Threads · Redis Atomic Ops · Apache Kafka · PostgreSQL · OpenTelemetry

## Live Demo
- **API:** http://20.196.222.20:8080/api/v1/health
- **Tracing:** http://20.196.222.20:16686

## Verified Performance (k6 Load Test — Azure B2as v2, 2 vCPU, 8GB RAM)

| Metric | Result |
|--------|--------|
| Requests per Second | **745 RPS** |
| P95 Latency | **93.47ms** |
| P90 Latency | 68.37ms |
| Average Latency | 26.56ms |
| Error Rate | **0.00%** |
| Total Requests | 134,234 |
| Max Virtual Users | 200 |
| Test Duration | 3 minutes |

## Architecture
[ 200 Concurrent Users ]

│

▼

Spring Boot 3.4 (Java 21 Virtual Threads)

│

┌────┴────┐

▼         ▼

Redis      Kafka

(Atomic)  (Async Buffer)

│

▼

PostgreSQL

(Batch Insert)

## Tech Stack
- **Java 21** — Virtual Threads (Project Loom)
- **Spring Boot 3.4** — REST API
- **Redis 7.2** — Atomic INCRBY balance validation
- **Apache Kafka 7.5** — 6-partition async event bus
- **PostgreSQL 16** — Batch persistence
- **OpenTelemetry + Jaeger** — Distributed tracing
- **k6** — Load testing

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/health` | GET | System health |
| `/api/v1/transactions` | POST | Process debit/credit |
| `/api/v1/accounts/{id}/balance` | GET | Sub-ms Redis balance |
| `/api/v1/accounts/{id}/seed` | POST | Seed test account |

## Quick Start
```bash
docker compose up -d
mvn spring-boot:run -Dspring-boot.run.jvmArguments="-Duser.timezone=UTC"
```

## Tests
- **Unit Tests:** 4 passing (Mockito + JUnit 5)
- **Load Tests:** k6 — 745 RPS, P95 93ms, 0% errors
