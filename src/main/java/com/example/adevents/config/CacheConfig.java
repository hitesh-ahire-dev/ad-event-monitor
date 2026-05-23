package com.example.adevents.config;

import com.example.adevents.model.DynamicEnrichment;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Caffeine cache for the dynamic enrichment lookup.
 * Short TTL so we never serve very stale runtime data while still
 * absorbing bursts (multiple events for the same eventId).
 */
@Configuration
public class CacheConfig {

    @Value("${app.dynamic-enrichment.cache-ttl-seconds:60}")
    private long ttlSeconds;

    @Value("${app.dynamic-enrichment.cache-max-size:50000}")
    private long maxSize;

    @Bean
    public com.github.benmanes.caffeine.cache.Cache<String, DynamicEnrichment> dynamicEnrichmentCache() {
        return Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(ttlSeconds))
                .maximumSize(maxSize)
                .recordStats()
                .build();
    }
}
