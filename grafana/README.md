# AD Event Monitor — Grafana Dashboard

Minimal, production-ready dashboard for the `ad-event-monitor` service.
Six panels, four sections, three alert rules.

## What the service exposes

After the changes in `KafkaConfig.java`, `AdEventConsumer.java` and
`application.yml`, the following endpoint is up:

```
GET http://localhost:8080/actuator/prometheus
```

It publishes:

| Metric                                  | Type           | Tags                          | Source |
|-----------------------------------------|----------------|-------------------------------|--------|
| `kafka_consumer_fetch_manager_records_lag_max`     | gauge   | `client_id`, `topic`        | `MicrometerConsumerListener` |
| `kafka_consumer_fetch_manager_records_consumed_total` | counter | `client_id`, `topic`     | `MicrometerConsumerListener` |
| `ad_events_processed_total`             | counter        | `result=success/failure/skipped` | `AdEventConsumer` |
| `ad_events_processing_latency`          | timer (p50/95/99) | —                          | `AdEventConsumer` |
| `ad_enrichment_total`                   | counter        | `type=static/dynamic`, `result=success/failure` | `AdEventConsumer` |
| `ad_enrichment_latency_seconds`         | timer          | `type=static/dynamic`         | `AdEventConsumer` |
| `ad_events_exceptions_total`            | counter        | `stage=...`                   | `AdEventConsumer` |
| `jvm_memory_used_bytes`, `jvm_memory_committed_bytes` | gauge | `area`                | Spring Boot Actuator |

All metrics carry the common tag `application="ad-event-monitor"` (set by
`management.metrics.tags.application`).

## Dashboard layout (6 panels, 4 rows)

```
┌── Kafka ───────────────────────────────────────────────────────┐
│  Consumer lag                 │ Messages consumed / sec        │
├── Event Processing ────────────────────────────────────────────┤
│  Total processed (stat)       │ Failure rate (success vs fail) │
├── Enrichment ──────────────────────────────────────────────────┤
│  Static vs dynamic outcomes   │ Enrichment latency p95         │
├── Errors & System ─────────────────────────────────────────────┤
│  Exceptions / sec by stage    │ JVM heap (used vs committed)   │
└────────────────────────────────────────────────────────────────┘
```

## Panel queries (PromQL)

| # | Panel | Query |
|---|-------|-------|
| 1 | Consumer lag | `max by (client_id) (kafka_consumer_fetch_manager_records_lag_max{application="$app"})` |
| 2 | Messages consumed / sec | `sum by (topic) (rate(kafka_consumer_fetch_manager_records_consumed_total{application="$app"}[1m]))` |
| 3 | Total events processed | `sum(ad_events_processed_total{application="$app"})` |
| 4 | Failure rate | `sum by (result) (rate(ad_events_processed_total{application="$app"}[1m]))` |
| 5 | Enrichment success vs failure | `sum by (type, result) (rate(ad_enrichment_total{application="$app"}[1m]))` |
| 6 | Enrichment latency p95 | `histogram_quantile(0.95, sum by (le, type) (rate(ad_enrichment_latency_seconds_bucket{application="$app"}[5m])))` |
| 7 | Exceptions / sec | `sum by (stage) (rate(ad_events_exceptions_total{application="$app"}[1m]))` |
| 8 | JVM heap | `sum(jvm_memory_used_bytes{application="$app", area="heap"})` and `…committed_bytes…` |

## Alert rules (see `alerts.yml`)

| Alert | Condition | For | Severity |
|-------|-----------|-----|----------|
| `HighKafkaLag`              | `records_lag_max > 5000`               | 2 m | warning  |
| `HighEventFailureRate`      | `failure ratio > 5%`                   | 5 m | critical |
| `HighEnrichmentFailureRate` | per-type `failure ratio > 10%`         | 5 m | warning  |

## Run Prometheus + Grafana locally

```bash
# Prometheus
docker run --rm -p 9090:9090 \
  -v $PWD/grafana/prometheus.yml:/etc/prometheus/prometheus.yml \
  -v $PWD/grafana/alerts.yml:/etc/prometheus/alerts.yml \
  prom/prometheus

# Grafana
docker run --rm -p 3000:3000 grafana/grafana
```

In Grafana (`http://localhost:3000`, admin/admin):
1. Add Prometheus data source: `http://host.docker.internal:9090`.
2. **Dashboards → Import** and paste `grafana/ad-event-monitor-dashboard.json`.
3. Select the Prometheus data source when prompted.

## Best practices applied

- **One signal per panel.** No mixing throughput with errors with latency.
- **Counters surfaced as rates**, not totals, except for the single "Total processed" stat panel.
- **Latency shown as p95**, not avg. The full p50/p95/p99 buckets are still recorded so you can drill in.
- **Result/stage labels keep cardinality low** (success/failure/skipped, 5 stage values). No `eventId` tag — that would explode cardinality.
- **One system panel only** (heap). CPU, threads, GC are *available* (Spring Boot exposes them) but kept off this dashboard to stay focused.
- **`$app` variable**, so the same JSON works for staging / prod by changing the value.
- **Three alerts, not thirty.** Lag, overall failure ratio, per-type enrichment failure ratio. Everything else is "look at the dashboard" territory.
