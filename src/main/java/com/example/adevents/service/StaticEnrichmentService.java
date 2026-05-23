package com.example.adevents.service;

import com.example.adevents.model.StaticEnrichment;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Loads the (large) static enrichment JSON into memory once at startup
 * and refreshes it every {@code app.static-enrichment.refresh-ms} (default 30 min).
 *
 * <p>The map is held behind an {@link AtomicReference} so reload is a
 * lock-free swap — readers never block.
 */
@Service
public class StaticEnrichmentService {

    private static final Logger log = LoggerFactory.getLogger(StaticEnrichmentService.class);

    private final ResourceLoader resourceLoader;
    private final ObjectMapper objectMapper;

    @Value("${app.static-enrichment.location}")
    private String location;

    private final AtomicReference<Map<String, StaticEnrichment>> ref =
            new AtomicReference<>(Collections.emptyMap());

    public StaticEnrichmentService(ResourceLoader resourceLoader, ObjectMapper objectMapper) {
        this.resourceLoader = resourceLoader;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void init() {
        reload();
    }

    /** Refresh every 30 minutes by default. */
    @Scheduled(fixedDelayString = "${app.static-enrichment.refresh-ms:1800000}")
    public void reload() {
        try {
            Resource resource = resourceLoader.getResource(location);
            if (!resource.exists()) {
                log.warn("Static enrichment resource not found at {}; keeping previous map (size={})",
                        location, ref.get().size());
                return;
            }
            try (InputStream in = resource.getInputStream()) {
                Map<String, StaticEnrichment> loaded = objectMapper.readValue(
                        in, new TypeReference<Map<String, StaticEnrichment>>() {});
                ref.set(loaded);
                log.info("Loaded static enrichment map: {} entries from {}", loaded.size(), location);
            }
        } catch (Exception e) {
            log.error("Failed to (re)load static enrichment from {}: {}", location, e.getMessage(), e);
        }
    }

    /** Returns the static enrichment for the given event id, or {@code null} if absent. */
    public StaticEnrichment getByEventId(String eventId) {
        if (eventId == null) return null;
        return ref.get().get(eventId);
    }

    int size() {
        return ref.get().size();
    }
}
