package com.example.adevents.consumer;

import com.example.adevents.model.AdEvent;
import com.example.adevents.model.DynamicEnrichment;
import com.example.adevents.model.StaticEnrichment;
import com.example.adevents.service.DynamicEnrichmentService;
import com.example.adevents.service.EventPersistenceService;
import com.example.adevents.service.StaticEnrichmentService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Kafka listener for the {@code ad-events} topic.
 * <p>
 * For each event it runs static and dynamic enrichment in parallel
 * (independent failures), then persists both enriched rows.
 * Per the requirement, when both enrichments are available we persist
 * them atomically via {@link EventPersistenceService#saveBoth}; if one
 * enrichment fails, we still persist the other in its own transaction.
 */
@Component
public class AdEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(AdEventConsumer.class);

    private final StaticEnrichmentService staticService;
    private final DynamicEnrichmentService dynamicService;
    private final EventPersistenceService persistence;
    private final Executor enrichmentExecutor;
    private final MeterRegistry meters;
    private final Timer processingTimer;

    public AdEventConsumer(StaticEnrichmentService staticService,
                           DynamicEnrichmentService dynamicService,
                           EventPersistenceService persistence,
                           @Qualifier("enrichmentExecutor") Executor enrichmentExecutor,
                           MeterRegistry meters) {
        this.staticService = staticService;
        this.dynamicService = dynamicService;
        this.persistence = persistence;
        this.enrichmentExecutor = enrichmentExecutor;
        this.meters = meters;
        this.processingTimer = Timer.builder("ad_events_processing_latency")
                .description("End-to-end processing latency per AD event")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(meters);
    }

    @KafkaListener(
            topics = "${app.kafka.topic:ad-events}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(AdEvent event) {
        if (event == null || event.getEventId() == null) {
            log.warn("Skipping null / id-less event: {}", event);
            meters.counter("ad_events_processed_total", "result", "skipped").increment();
            return;
        }

        String eventId = event.getEventId();
        Timer.Sample sample = Timer.start(meters);

        CompletableFuture<StaticEnrichment> sf = CompletableFuture
                .supplyAsync(() -> meters
                        .timer("ad_enrichment_latency_seconds", "type", "static")
                        .record(() -> staticService.getByEventId(eventId)),
                        enrichmentExecutor)
                .exceptionally(ex -> {
                    log.error("Static enrichment failed for eventId={}: {}", eventId, ex.getMessage());
                    meters.counter("ad_enrichment_total", "type", "static", "result", "failure").increment();
                    meters.counter("ad_events_exceptions_total", "stage", "static_enrichment").increment();
                    return null;
                });

        CompletableFuture<DynamicEnrichment> df = CompletableFuture
                .supplyAsync(() -> meters
                        .timer("ad_enrichment_latency_seconds", "type", "dynamic")
                        .record(() -> dynamicService.getByEventId(eventId)),
                        enrichmentExecutor)
                .exceptionally(ex -> {
                    log.error("Dynamic enrichment failed for eventId={}: {}", eventId, ex.getMessage());
                    meters.counter("ad_enrichment_total", "type", "dynamic", "result", "failure").increment();
                    meters.counter("ad_events_exceptions_total", "stage", "dynamic_enrichment").increment();
                    return null;
                });

        StaticEnrichment se = sf.join();
        DynamicEnrichment de = df.join();

        meters.counter("ad_enrichment_total", "type", "static",
                "result", se != null ? "success" : "failure").increment();
        meters.counter("ad_enrichment_total", "type", "dynamic",
                "result", de != null ? "success" : "failure").increment();

        log.info("Processed enrichments eventId={} static={} dynamic={}",
                eventId, se != null, de != null);

        boolean persisted = true;
        try {
            persistence.saveBoth(event, se, de);
        } catch (Exception bothFailed) {
            log.error("Atomic save failed for eventId={}, falling back to independent saves: {}",
                    eventId, bothFailed.getMessage());
            meters.counter("ad_events_exceptions_total", "stage", "persist_atomic").increment();
            persisted = false;
            try {
                persistence.saveStatic(event, se);
                persisted = true;
            } catch (Exception e) {
                log.error("Static save failed for eventId={}: {}", eventId, e.getMessage());
                meters.counter("ad_events_exceptions_total", "stage", "persist_static").increment();
            }
            try {
                persistence.saveDynamic(event, de);
                persisted = true;
            } catch (Exception e) {
                log.error("Dynamic save failed for eventId={}: {}", eventId, e.getMessage());
                meters.counter("ad_events_exceptions_total", "stage", "persist_dynamic").increment();
            }
        }

        meters.counter("ad_events_processed_total",
                "result", persisted ? "success" : "failure").increment();
        sample.stop(processingTimer);
    }
}
