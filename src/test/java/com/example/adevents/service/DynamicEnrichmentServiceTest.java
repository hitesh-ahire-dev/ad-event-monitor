package com.example.adevents.service;

import com.example.adevents.model.DynamicEnrichment;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ResourceLoader;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class DynamicEnrichmentServiceTest {

    @Test
    void cachesAfterFirstFetch() {
        AtomicInteger calls = new AtomicInteger();
        Cache<String, DynamicEnrichment> cache = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(60)).maximumSize(100).build();

        DynamicEnrichmentService svc = new DynamicEnrichmentService(
                mock(ResourceLoader.class), new ObjectMapper(), cache) {
            @Override
            protected DynamicEnrichment fetchFromSource(String eventId) {
                calls.incrementAndGet();
                return DynamicEnrichment.builder()
                        .riskScore(50).sessionActive(true)
                        .policyViolation(false).lastSeenMinutesAgo(2).build();
            }
        };

        DynamicEnrichment a = svc.getByEventId("EVT-1");
        DynamicEnrichment b = svc.getByEventId("EVT-1");

        assertThat(a).isNotNull().isSameAs(b);
        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void nullEventIdReturnsNull() {
        Cache<String, DynamicEnrichment> cache = Caffeine.newBuilder().maximumSize(1).build();
        DynamicEnrichmentService svc = new DynamicEnrichmentService(
                mock(ResourceLoader.class), new ObjectMapper(), cache) {
            @Override protected DynamicEnrichment fetchFromSource(String eventId) { return null; }
        };
        assertThat(svc.getByEventId(null)).isNull();
    }
}
