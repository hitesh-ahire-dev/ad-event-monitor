package com.example.adevents.consumer;

import com.example.adevents.model.AdEvent;
import com.example.adevents.model.DynamicEnrichment;
import com.example.adevents.model.StaticEnrichment;
import com.example.adevents.service.DynamicEnrichmentService;
import com.example.adevents.service.EventPersistenceService;
import com.example.adevents.service.StaticEnrichmentService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class AdEventConsumerTest {

    private StaticEnrichmentService staticSvc;
    private DynamicEnrichmentService dynamicSvc;
    private EventPersistenceService persistence;
    private AdEventConsumer consumer;

    @BeforeEach
    void setup() {
        staticSvc = mock(StaticEnrichmentService.class);
        dynamicSvc = mock(DynamicEnrichmentService.class);
        persistence = mock(EventPersistenceService.class);
        consumer = new AdEventConsumer(staticSvc, dynamicSvc, persistence,
                Runnable::run, new SimpleMeterRegistry());
    }

    private AdEvent sample() {
        return AdEvent.builder()
                .eventId("EVT-1001").eventType("USER_LOGIN").userId("u1")
                .timestamp(Instant.parse("2024-01-15T10:30:00Z"))
                .sourceIp("1.2.3.4").domain("corp.example.com").build();
    }

    @Test
    void persistsBothWhenEnrichmentsResolve() {
        StaticEnrichment se = StaticEnrichment.builder().department("Eng").build();
        DynamicEnrichment de = DynamicEnrichment.builder().riskScore(72).build();
        when(staticSvc.getByEventId("EVT-1001")).thenReturn(se);
        when(dynamicSvc.getByEventId("EVT-1001")).thenReturn(de);

        consumer.onMessage(sample());

        verify(persistence).saveBoth(any(AdEvent.class), eq(se), eq(de));
        verify(persistence, never()).saveStatic(any(), any());
        verify(persistence, never()).saveDynamic(any(), any());
    }

    @Test
    void nullEnrichmentsArePersistedAsNulls() {
        when(staticSvc.getByEventId("EVT-1001")).thenReturn(null);
        when(dynamicSvc.getByEventId("EVT-1001")).thenReturn(null);

        consumer.onMessage(sample());

        ArgumentCaptor<StaticEnrichment> sc = ArgumentCaptor.forClass(StaticEnrichment.class);
        ArgumentCaptor<DynamicEnrichment> dc = ArgumentCaptor.forClass(DynamicEnrichment.class);
        verify(persistence).saveBoth(any(AdEvent.class), sc.capture(), dc.capture());
        assertThat(sc.getValue()).isNull();
        assertThat(dc.getValue()).isNull();
    }

    @Test
    void enrichmentExceptionDoesNotBlockOther() {
        when(staticSvc.getByEventId("EVT-1001")).thenThrow(new RuntimeException("boom"));
        DynamicEnrichment de = DynamicEnrichment.builder().riskScore(10).build();
        when(dynamicSvc.getByEventId("EVT-1001")).thenReturn(de);

        consumer.onMessage(sample());

        verify(persistence).saveBoth(any(AdEvent.class), eq(null), eq(de));
    }

    @Test
    void atomicSaveFailureFallsBackToIndependentSaves() {
        StaticEnrichment se = StaticEnrichment.builder().department("Eng").build();
        DynamicEnrichment de = DynamicEnrichment.builder().riskScore(72).build();
        when(staticSvc.getByEventId("EVT-1001")).thenReturn(se);
        when(dynamicSvc.getByEventId("EVT-1001")).thenReturn(de);
        doThrow(new RuntimeException("db blip")).when(persistence).saveBoth(any(), any(), any());

        consumer.onMessage(sample());

        verify(persistence).saveStatic(any(AdEvent.class), eq(se));
        verify(persistence).saveDynamic(any(AdEvent.class), eq(de));
    }

    @Test
    void nullOrIdLessEventIsSkipped() {
        consumer.onMessage(null);
        consumer.onMessage(new AdEvent());
        verifyNoInteractions(persistence);
    }
}
