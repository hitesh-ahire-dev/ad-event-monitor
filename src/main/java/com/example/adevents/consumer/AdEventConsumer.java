package com.example.adevents.consumer;

import com.example.adevents.model.AdEvent;
import com.example.adevents.model.DynamicEnrichment;
import com.example.adevents.model.StaticEnrichment;
import com.example.adevents.service.DynamicEnrichmentService;
import com.example.adevents.service.EventPersistenceService;
import com.example.adevents.service.StaticEnrichmentService;
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

    public AdEventConsumer(StaticEnrichmentService staticService,
                           DynamicEnrichmentService dynamicService,
                           EventPersistenceService persistence,
                           @Qualifier("enrichmentExecutor") Executor enrichmentExecutor) {
        this.staticService = staticService;
        this.dynamicService = dynamicService;
        this.persistence = persistence;
        this.enrichmentExecutor = enrichmentExecutor;
    }

    @KafkaListener(
            topics = "${app.kafka.topic:ad-events}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(AdEvent event) {
        if (event == null || event.getEventId() == null) {
            log.warn("Skipping null / id-less event: {}", event);
            return;
        }

        String eventId = event.getEventId();

        CompletableFuture<StaticEnrichment> sf = CompletableFuture
                .supplyAsync(() -> staticService.getByEventId(eventId), enrichmentExecutor)
                .exceptionally(ex -> {
                    log.error("Static enrichment failed for eventId={}: {}", eventId, ex.getMessage());
                    return null;
                });

        CompletableFuture<DynamicEnrichment> df = CompletableFuture
                .supplyAsync(() -> dynamicService.getByEventId(eventId), enrichmentExecutor)
                .exceptionally(ex -> {
                    log.error("Dynamic enrichment failed for eventId={}: {}", eventId, ex.getMessage());
                    return null;
                });

        StaticEnrichment se = sf.join();
        DynamicEnrichment de = df.join();

        log.info("Processed enrichments eventId={} static={} dynamic={}",
                eventId, se != null, de != null);

        try {
            persistence.saveBoth(event, se, de);
        } catch (Exception bothFailed) {
            log.error("Atomic save failed for eventId={}, falling back to independent saves: {}",
                    eventId, bothFailed.getMessage());
            try {
                persistence.saveStatic(event, se);
            } catch (Exception e) {
                log.error("Static save failed for eventId={}: {}", eventId, e.getMessage());
            }
            try {
                persistence.saveDynamic(event, de);
            } catch (Exception e) {
                log.error("Dynamic save failed for eventId={}: {}", eventId, e.getMessage());
            }
        }
    }
}
