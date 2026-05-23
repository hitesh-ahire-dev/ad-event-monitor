# ad-event-monitor

Real-time Active Directory event monitoring & enrichment service.
Consumes events from the Kafka topic `ad-events`, enriches each event with
**static** (reference data) and **dynamic** (live runtime) info, and stores the
two enriched views in independent tables.

## Project layout

```
ad-event-monitor/
├── pom.xml
├── src/main/java/com/example/adevents/
│   ├── AdEventMonitorApplication.java
│   ├── config/
│   │   ├── KafkaConfig.java          # consumer factory + error handler (DLT-by-skip)
│   │   ├── CacheConfig.java          # Caffeine cache for dynamic enrichment
│   │   └── AsyncConfig.java          # thread pool used to fan-out enrichments
│   ├── model/
│   │   ├── AdEvent.java
│   │   ├── StaticEnrichment.java
│   │   └── DynamicEnrichment.java
│   ├── entity/
│   │   ├── StaticEnrichedEvent.java
│   │   └── DynamicEnrichedEvent.java
│   ├── repository/
│   │   ├── StaticEnrichedEventRepository.java
│   │   └── DynamicEnrichedEventRepository.java
│   ├── service/
│   │   ├── StaticEnrichmentService.java
│   │   ├── DynamicEnrichmentService.java
│   │   └── EventPersistenceService.java
│   └── consumer/
│       └── AdEventConsumer.java
├── src/main/resources/
│   ├── application.yml
│   ├── schema.sql
│   ├── static-enrichment.json
│   └── dynamic-enrichment.json
└── src/test/...
```

## Layer-by-layer rationale

### 1. Model / Entity
Plain POJOs (`AdEvent`, `StaticEnrichment`, `DynamicEnrichment`) describe wire
formats and stay free of JPA annotations so they can be serialized/deserialized
freely.
JPA entities (`StaticEnrichedEvent`, `DynamicEnrichedEvent`) describe table
shape only; keeping them separate from the wire models prevents Hibernate proxy
quirks from leaking into Jackson serialization.

### 2. Repository
Vanilla Spring Data JPA `JpaRepository`s — one per table, mirroring the
"two independent enrichment views" requirement.

### 3. Enrichment service
* **`StaticEnrichmentService`** loads a possibly-large (~100k keys) JSON
  document at startup into an `AtomicReference<Map<...>>` so reads are
  lock-free and refreshes (every 30 min by `@Scheduled`) atomically swap the
  map. This satisfies *"loaded into memory, not fetched per event"*.
* **`DynamicEnrichmentService`** fronts a per-event lookup with a Caffeine
  cache (TTL 60 s, bounded size). The reference impl reads a JSON file on
  miss; in production swap `fetchFromSource` for a REST/gRPC call.

### 4. Kafka consumer
`AdEventConsumer` runs the two enrichments in parallel via
`CompletableFuture` on a dedicated executor. Each enrichment future has its
own `exceptionally` so a failure in one **does not block the other**.
After both futures complete it calls `EventPersistenceService.saveBoth` which
writes both rows in **one transaction**; if the atomic save itself fails it
falls back to two `REQUIRES_NEW` saves so a single bad row can never wipe out
the other side.

### 5. Configuration
`application.yml` centralises Kafka, JPA/Postgres, static-JSON location, and
cache TTL. `KafkaConfig` wires an `ErrorHandlingDeserializer` so poison-pill
JSON does not crash the consumer; `DefaultErrorHandler` retries twice with
500 ms backoff and then skips (acts as a simple dead-letter).

### 6. Error handling
| Failure mode                       | Handling                                                |
|------------------------------------|---------------------------------------------------------|
| Kafka deserialization fails        | `ErrorHandlingDeserializer` → `DefaultErrorHandler` logs & skips |
| Static enrichment lookup → null    | Entity persisted with null enrichment columns           |
| Dynamic enrichment lookup → null   | Entity persisted with null enrichment columns           |
| One enrichment throws              | Logged; the other still persists                        |
| Atomic DB save fails               | Falls back to per-side `REQUIRES_NEW` saves             |
| Per-side DB save fails             | Logged with `eventId`; consumer continues               |

### 7. Tests
* **Unit** – `StaticEnrichmentServiceTest`, `DynamicEnrichmentServiceTest`,
  `AdEventConsumerTest` (synchronous executor, Mockito-mocked services/repos).
* **Integration** – `AdEventIntegrationTest` boots Kafka + Postgres via
  Testcontainers, publishes one event, and asserts that both tables hold
  exactly one row with the expected enriched fields.

## `@Async` / parallel-enrichment trade-offs

Pros
* Static (in-memory map) and dynamic (cache or remote) run concurrently, so
  end-to-end latency ≈ `max(static, dynamic)` instead of their sum.

Cons
* Each in-flight event ties up an executor thread; with many Kafka partitions
  & `concurrency` already giving you parallelism, the marginal gain shrinks.
* Per-partition Kafka ordering can be lost if you ack before the futures
  finish — we wait (`join()`) before save & ack to keep order.
* A misbehaving downstream can fill the executor queue; the bounded queue
  + caller-runs-style sizing in `AsyncConfig` is what stops heap-blowups.

## Running

```bash
# 1. Postgres + Kafka up (any way you like; e.g. docker compose)
# 2. Build & run
mvn spring-boot:run
```

```bash
# Run tests (integration test needs Docker)
mvn test
```
