# `ad-event-monitor` — End-to-End Workflow

Real-time AD (Active Directory) event monitoring & enrichment service.

This document describes the full data flow — **from a Kafka message landing
on the `ad-events` topic, through enrichment, persistence, and finally
serving the enriched data over a REST API documented with Swagger / OpenAPI.**

---

## 1. High-level architecture

```
┌────────────────┐         ┌───────────────────────────────────────────────────────┐
│  Kafka topic   │         │                 ad-event-monitor (Spring Boot)        │
│   ad-events    │ ──▶──▶  │                                                       │
└────────────────┘         │   ┌─────────────────┐                                 │
                           │   │ AdEventConsumer │  ① deserialize AdEvent          │
                           │   └────────┬────────┘                                 │
                           │            │ eventId                                  │
                           │   ┌────────┴───────────────────────────┐              │
                           │   ▼ parallel CompletableFutures        ▼              │
                           │ ┌────────────────────────┐ ┌──────────────────────┐   │
                           │ │ StaticEnrichmentService│ │DynamicEnrichmentSvc  │   │
                           │ │ (in-mem Map, 30m refr.)│ │(Caffeine 60s TTL +   │   │
                           │ │                        │ │ live source)         │   │
                           │ └────────────┬───────────┘ └──────────┬───────────┘   │
                           │              │ StaticEnrichment       │ DynamicEnr.   │
                           │              └────────┬───────────────┘               │
                           │                       ▼                               │
                           │           ┌───────────────────────┐                   │
                           │           │ EventPersistenceSvc   │  ② saveBoth (1 tx)│
                           │           │  - fallback per-side  │     or independent│
                           │           └─────────┬─────────────┘     REQUIRES_NEW  │
                           │                     │                                 │
                           │                     ▼                                 │
                           │   ┌─────────────────────────┐  ┌─────────────────────┐│
                           │   │ static_enriched_events  │  │dynamic_enriched_evt ││
                           │   │      (MySQL)            │  │     (MySQL)         ││
                           │   └────────┬────────────────┘  └──────────┬──────────┘│
                           │            │                              │           │
                           │            └──────────────┬───────────────┘           │
                           │                           ▼                           │
                           │             ┌──────────────────────────┐              │
                           │             │ REST controllers + DTOs  │  ③ GET ...   │
                           │             │ /api/v1/static-events    │              │
                           │             │ /api/v1/dynamic-events   │              │
                           │             └────────────┬─────────────┘              │
                           │                          ▼                            │
                           │            Swagger UI  /swagger-ui/index.html         │
                           └───────────────────────────────────────────────────────┘
```

---

## 2. Component-by-component flow

### Stage ① — Ingestion (Kafka → POJO)

| Where | What it does |
|------|--------------|
| `KafkaConfig.consumerFactory()` | Builds a `ConsumerFactory<String, AdEvent>` that wraps a `JsonDeserializer<AdEvent>` constructed with Spring Boot's `ObjectMapper` (so `JavaTimeModule` is registered → `Instant` parsing works). Both key and value go through `ErrorHandlingDeserializer`, so poison-pill payloads do not kill the consumer. |
| `KafkaConfig.kafkaListenerContainerFactory()` | Adds a `DefaultErrorHandler` with 2 × 500 ms back-off; after retries the bad record is logged and skipped (simple dead-letter-by-skip). |
| `KafkaConfig.adEventsTopic()` | Auto-creates the `ad-events` topic via `KafkaAdmin` at startup. No-op if it already exists. |
| `AdEventConsumer.onMessage(AdEvent)` | Entry point. Skips events with no `eventId` and logs a warning. |

### Stage ② — Enrichment (parallel, independent failure)

`AdEventConsumer` launches two `CompletableFuture`s on the `enrichmentExecutor` thread pool:

1. **Static enrichment** — `StaticEnrichmentService.getByEventId(eventId)`
   - The full JSON (`classpath:static-enrichment.json` or whatever
     `app.static-enrichment.location` points to) is loaded once at startup
     into an `AtomicReference<Map<String, StaticEnrichment>>`.
   - `@Scheduled(fixedDelayString = "${app.static-enrichment.refresh-ms:1800000}")`
     reloads it every 30 minutes via an atomic swap. Readers never block.
   - Returns `null` for unknown ids (the enriched row will still be saved
     with null enrichment columns).

2. **Dynamic enrichment** — `DynamicEnrichmentService.getByEventId(eventId)`
   - Fronted by a Caffeine cache (`expireAfterWrite = 60 s`,
     `maximumSize = 50 000` by default).
   - On cache miss it calls `fetchFromSource(eventId)`. In this reference
     implementation that reads the JSON file; in production you would
     replace the method with a REST/gRPC call to the live risk service.
   - Returns `null` for unknown ids.

Each future has its own `.exceptionally(...)` so a failure in one **does not
block the other** — the consumer always proceeds with whatever it has,
substituting `null` for the side that failed.

### Stage ③ — Persistence (atomic; fallback on partial failure)

`EventPersistenceService` exposes three methods:

| Method | Transactional semantics |
|--------|--------------------------|
| `saveBoth(event, se, de)` | `@Transactional` (REQUIRED). One transaction, both INSERTs commit together. Either both rows land in MySQL or neither does. |
| `saveStatic(event, se)`   | `@Transactional(REQUIRES_NEW)` — used as fallback when `saveBoth` fails so the static side can still be persisted independently. |
| `saveDynamic(event, de)`  | `@Transactional(REQUIRES_NEW)` — same idea for the dynamic side. |

The consumer's flow:

```text
try {
    persistence.saveBoth(...);            // atomic
} catch (Exception bothFailed) {
    persistence.saveStatic(...);          // each in its own tx
    persistence.saveDynamic(...);
}
```

So we honour the constraint *"both enriched records must be written in the
same transaction per event, or handle partial failure explicitly"*.

### Stage ④ — Tables persisted

| Table | Columns |
|-------|---------|
| `static_enriched_events` | `id, event_id, event_type, user_id, event_timestamp, source_ip, domain, department, role, site, manager_email, created_at` |
| `dynamic_enriched_events` | `id, event_id, event_type, user_id, event_timestamp, source_ip, domain, risk_score, session_active, policy_violation, last_seen_minutes_ago, created_at` |

`event_timestamp` is used instead of `timestamp` to avoid the MySQL reserved word.
`schema.sql` is auto-run via `spring.sql.init.mode=always`.

### Stage ⑤ — Query layer (REST + Swagger)

| Layer | Class |
|-------|-------|
| DTO records (never expose entities) | `StaticEnrichedEventResponse`, `DynamicEnrichedEventResponse`, `ErrorResponse` |
| Query service                       | `StaticEnrichedEventQueryService`, `DynamicEnrichedEventQueryService` (read-only Tx) |
| Repository                          | `findAll()` + `findByEventId(...)` → delegates to `findTopByEventIdOrderByIdDesc(...)` so the latest row wins, **queried in the DB**, not filtered in memory. |
| Controller                          | `StaticEnrichedEventController`, `DynamicEnrichedEventController` — thin: no business logic, just calls the service. |
| Error handler                       | `GlobalExceptionHandler` — `EventNotFoundException → 404`, `MethodArgumentTypeMismatchException → 400`, anything else → `500`. Body is `ErrorResponse{ error, timestamp }`. |
| OpenAPI                             | `OpenApiConfig` + `springdoc-openapi-starter-webmvc-ui` → Swagger UI at `/swagger-ui/index.html`, JSON at `/v3/api-docs`. |

REST endpoints:

| Method | Path | Description | Codes |
|--------|------|-------------|-------|
| GET | `/api/v1/static-events` | All static-enriched events | 200 |
| GET | `/api/v1/static-events/{eventId}` | Latest static-enriched event for the id | 200 / 404 |
| GET | `/api/v1/dynamic-events` | All dynamic-enriched events | 200 |
| GET | `/api/v1/dynamic-events/{eventId}` | Latest dynamic-enriched event for the id | 200 / 404 |

---

## 3. Configuration knobs (`application.yml`)

| Key | Default | Purpose |
|-----|---------|---------|
| `spring.kafka.bootstrap-servers` | `localhost:9092` | Where the Kafka broker lives. |
| `spring.kafka.consumer.group-id` | `ad-event-monitor` | Consumer group. |
| `app.kafka.topic` | `ad-events` | Topic auto-created at startup. |
| `app.kafka.partitions` / `app.kafka.replicas` | `1` / `1` | Topic shape. |
| `app.static-enrichment.location` | `classpath:static-enrichment.json` | Where to load the big static map from. |
| `app.static-enrichment.refresh-ms` | `1800000` (30 m) | How often `@Scheduled` reloads it. |
| `app.dynamic-enrichment.location` | `classpath:dynamic-enrichment.json` | Dev source for per-event lookups. |
| `app.dynamic-enrichment.cache-ttl-seconds` | `60` | Caffeine TTL. |
| `app.dynamic-enrichment.cache-max-size` | `50000` | Caffeine max entries. |
| `spring.datasource.url` | `jdbc:mysql://localhost:3306/todo_app?...` | DB connection. |
| `springdoc.swagger-ui.path` | `/swagger-ui/index.html` | Swagger entrypoint. |

---

## 4. End-to-end walkthrough

### Prerequisites
- MySQL running on `localhost:3306` with user `root / root1234` (or update `application.yml`).
- Kafka broker running on `localhost:9092`.
- Java 17 + Maven.

### Run the service

```bash
mvn -f /Users/hiteshahire/Documents/java-workspace/ad-event-monitor/pom.xml \
    spring-boot:run
```

On startup you should see:
- `Loaded static enrichment map: N entries from classpath:static-enrichment.json`
- `Tomcat started on port(s): 8080`
- Topic `ad-events` is created automatically (no-op if it already existed).

### Produce sample events

Run the dedicated test producer (gated by `-Dkafka.publish=true` so it
never runs during a regular build):

```bash
mvn -f /Users/hiteshahire/Documents/java-workspace/ad-event-monitor/pom.xml test \
    -Dtest=AdEventProducerIT \
    -Dkafka.publish=true \
    -Dkafka.count=20
```

What it does:
- Sends 20 JSON `AdEvent` messages cycling through `EVT-1001`, `EVT-1002`,
  `EVT-9999`. The first two have entries in both static and dynamic JSON;
  `EVT-9999` exercises the **null-enrichment** path.
- Uses an `ObjectMapper` with `JavaTimeModule` and
  `WRITE_DATES_AS_TIMESTAMPS=false`, so `timestamp` is serialized as an
  ISO-8601 string compatible with the consumer.

### Observe consumer logs

For each message you should see something like:

```
INFO  ... AdEventConsumer        : Processed enrichments eventId=EVT-1001 static=true dynamic=true
INFO  ... EventPersistenceService: Persisted both enriched rows for eventId=EVT-1001
INFO  ... AdEventConsumer        : Processed enrichments eventId=EVT-9999 static=false dynamic=false
INFO  ... EventPersistenceService: Persisted both enriched rows for eventId=EVT-9999
```

### Verify in MySQL

```sql
USE todo_app;
SELECT event_id, department, role, site FROM static_enriched_events ORDER BY id DESC LIMIT 5;
SELECT event_id, risk_score, session_active, policy_violation FROM dynamic_enriched_events ORDER BY id DESC LIMIT 5;
```

### Query the REST API

```bash
curl http://localhost:8080/api/v1/static-events | jq
curl http://localhost:8080/api/v1/static-events/EVT-1001 | jq
curl http://localhost:8080/api/v1/dynamic-events/EVT-1001 | jq

# 404 path
curl -i http://localhost:8080/api/v1/static-events/UNKNOWN
# HTTP/1.1 404
# { "error":"Static enriched event not found for eventId: UNKNOWN", "timestamp":"2024-…" }
```

### Browse the Swagger UI

Open <http://localhost:8080/swagger-ui/index.html>. Both controllers are
tagged (`Static Enriched Events`, `Dynamic Enriched Events`); each
operation has `@Operation` summary/description, response codes 200/404,
and realistic `@Schema` examples on every DTO field.

OpenAPI JSON is at <http://localhost:8080/v3/api-docs>.

---

## 5. Error handling matrix

| Failure                                  | Behaviour                                                                                       |
|------------------------------------------|-------------------------------------------------------------------------------------------------|
| Bad JSON on Kafka                        | `ErrorHandlingDeserializer` surfaces it as a header → `DefaultErrorHandler` retries × 2, then skips (acts as DLT). |
| `eventId` missing                        | Consumer logs a warning, returns — message is treated as consumed.                              |
| Static enrichment throws                 | Future captures it; consumer continues with `null` static enrichment (logged).                  |
| Dynamic enrichment throws                | Future captures it; consumer continues with `null` dynamic enrichment (logged).                 |
| Both enrichments return `null`           | Row is still persisted with `null` enrichment columns.                                          |
| Atomic `saveBoth` DB failure             | Caught; consumer falls back to two independent `REQUIRES_NEW` saves so one side can still land. |
| Either per-side save fails               | Logged with `eventId`; consumer keeps running.                                                  |
| REST: `EventNotFoundException`           | `404 { error, timestamp }`.                                                                     |
| REST: bad path variable type             | `400 { error: "Invalid parameter: …", timestamp }`.                                             |
| REST: unhandled exception                | `500 { error: "Internal server error", timestamp }`.                                            |

---

## 6. Concurrency & ordering notes

- The two enrichments run in parallel on `enrichmentExecutor` (core 8 / max 16 /
  queue 500). End-to-end latency per event ≈ `max(static, dynamic)` instead
  of their sum.
- The consumer **always `.join()`s both futures before saving**, so per-Kafka-partition
  ordering is preserved.
- The Caffeine cache is bounded; a slow dynamic source cannot blow up heap.
- Static map updates happen via atomic reference swap → no reader contention,
  no half-loaded view.

---

## 7. File map (where to look in source)

| Concern | File |
|---------|------|
| Bootstrap | `src/main/java/com/example/adevents/AdEventMonitorApplication.java` |
| Kafka config | `src/main/java/com/example/adevents/config/KafkaConfig.java` |
| Caffeine cache | `src/main/java/com/example/adevents/config/CacheConfig.java` |
| Async executor | `src/main/java/com/example/adevents/config/AsyncConfig.java` |
| OpenAPI metadata | `src/main/java/com/example/adevents/config/OpenApiConfig.java` |
| Wire POJOs | `src/main/java/com/example/adevents/model/*.java` |
| JPA entities | `src/main/java/com/example/adevents/entity/*.java` |
| Repositories | `src/main/java/com/example/adevents/repository/*.java` |
| Enrichment services | `src/main/java/com/example/adevents/service/StaticEnrichmentService.java`, `DynamicEnrichmentService.java` |
| Persistence service | `src/main/java/com/example/adevents/service/EventPersistenceService.java` |
| Query services | `src/main/java/com/example/adevents/service/StaticEnrichedEventQueryService.java`, `DynamicEnrichedEventQueryService.java` |
| Kafka consumer | `src/main/java/com/example/adevents/consumer/AdEventConsumer.java` |
| REST controllers | `src/main/java/com/example/adevents/controller/*.java` |
| DTOs | `src/main/java/com/example/adevents/dto/*.java` |
| Exceptions | `src/main/java/com/example/adevents/exception/*.java` |
| Schema & seed JSON | `src/main/resources/schema.sql`, `static-enrichment.json`, `dynamic-enrichment.json` |
| App config | `src/main/resources/application.yml` |
| Unit tests | `src/test/java/com/example/adevents/service/*Test.java`, `controller/*Test.java`, `consumer/*Test.java` |
| Sample producer | `src/test/java/com/example/adevents/producer/AdEventProducerIT.java` |

---

## 8. Run all the things

```bash
# Build + run unit tests (no Docker, no broker needed)
mvn -f /Users/hiteshahire/Documents/java-workspace/ad-event-monitor/pom.xml clean install

# Start the service (needs MySQL + Kafka)
mvn -f /Users/hiteshahire/Documents/java-workspace/ad-event-monitor/pom.xml spring-boot:run

# Pump in 50 sample events
mvn -f /Users/hiteshahire/Documents/java-workspace/ad-event-monitor/pom.xml test \
    -Dtest=AdEventProducerIT -Dkafka.publish=true -Dkafka.count=50

# Read them back
curl -s http://localhost:8080/api/v1/static-events  | jq 'length'
curl -s http://localhost:8080/api/v1/dynamic-events | jq 'length'
open http://localhost:8080/swagger-ui/index.html
```
