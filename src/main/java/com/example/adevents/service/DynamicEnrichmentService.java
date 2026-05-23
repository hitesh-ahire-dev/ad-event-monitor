package com.example.adevents.service;

import com.example.adevents.model.DynamicEnrichment;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.Map;

/**
 * Per-event dynamic enrichment lookup, fronted by a Caffeine cache (TTL 60s by default).
 *
 * <p>For this reference implementation the underlying "source" is a JSON file
 * (re-read on cache miss). In production replace {@link #fetchFromSource(String)}
 * with a REST/gRPC call to the live risk/session service.
 */
@Service
public class DynamicEnrichmentService {

    private static final Logger log = LoggerFactory.getLogger(DynamicEnrichmentService.class);

    private final ResourceLoader resourceLoader;
    private final ObjectMapper objectMapper;
    private final Cache<String, DynamicEnrichment> cache;

    @Value("${app.dynamic-enrichment.location}")
    private String location;

    public DynamicEnrichmentService(ResourceLoader resourceLoader,
                                    ObjectMapper objectMapper,
                                    Cache<String, DynamicEnrichment> dynamicEnrichmentCache) {
        this.resourceLoader = resourceLoader;
        this.objectMapper = objectMapper;
        this.cache = dynamicEnrichmentCache;
    }

    /** Returns the dynamic enrichment for the given event id, or {@code null} if absent. */
    public DynamicEnrichment getByEventId(String eventId) {
        if (eventId == null) return null;
        // Cache#get loads on miss; Caffeine treats null returns by simply not caching.
        DynamicEnrichment cached = cache.getIfPresent(eventId);
        if (cached != null) return cached;
        DynamicEnrichment fresh = fetchFromSource(eventId);
        if (fresh != null) cache.put(eventId, fresh);
        return fresh;
    }

    /** Override / mock this in tests or production to call the live source. */
    protected DynamicEnrichment fetchFromSource(String eventId) {
        try {
            Resource resource = resourceLoader.getResource(location);
            if (!resource.exists()) {
                log.warn("Dynamic enrichment resource not found at {}", location);
                return null;
            }
            try (InputStream in = resource.getInputStream()) {
                Map<String, DynamicEnrichment> all = objectMapper.readValue(
                        in, new TypeReference<Map<String, DynamicEnrichment>>() {});
                return all.getOrDefault(eventId, null);
            }
        } catch (Exception e) {
            log.error("Dynamic enrichment fetch failed for eventId={}: {}", eventId, e.getMessage());
            return null;
        }
    }

    /** Test hook. */
    void clearCache() {
        cache.invalidateAll();
    }
}
