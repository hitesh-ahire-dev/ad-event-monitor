package com.example.adevents.service;

import com.example.adevents.entity.DynamicEnrichedEvent;
import com.example.adevents.entity.StaticEnrichedEvent;
import com.example.adevents.model.AdEvent;
import com.example.adevents.model.DynamicEnrichment;
import com.example.adevents.model.StaticEnrichment;
import com.example.adevents.repository.DynamicEnrichedEventRepository;
import com.example.adevents.repository.StaticEnrichedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Builds the two enriched entities and saves them.
 *
 * <p>{@link #saveBoth} is annotated {@link Transactional} so both rows commit
 * together; {@link #saveStatic} / {@link #saveDynamic} are independent
 * {@code REQUIRES_NEW} transactions used when the caller wants partial-failure
 * semantics (one enrichment can succeed even if the other fails).
 */
@Service
public class EventPersistenceService {

    private static final Logger log = LoggerFactory.getLogger(EventPersistenceService.class);

    private final StaticEnrichedEventRepository staticRepo;
    private final DynamicEnrichedEventRepository dynamicRepo;

    public EventPersistenceService(StaticEnrichedEventRepository staticRepo,
                                   DynamicEnrichedEventRepository dynamicRepo) {
        this.staticRepo = staticRepo;
        this.dynamicRepo = dynamicRepo;
    }

    @Transactional
    public void saveBoth(AdEvent event, StaticEnrichment se, DynamicEnrichment de) {
        staticRepo.save(toStaticEntity(event, se));
        dynamicRepo.save(toDynamicEntity(event, de));
        log.info("Persisted both enriched rows for eventId={}", event.getEventId());
    }

    @Transactional(propagation = org.springframework.transaction.annotation.Propagation.REQUIRES_NEW)
    public void saveStatic(AdEvent event, StaticEnrichment se) {
        staticRepo.save(toStaticEntity(event, se));
    }

    @Transactional(propagation = org.springframework.transaction.annotation.Propagation.REQUIRES_NEW)
    public void saveDynamic(AdEvent event, DynamicEnrichment de) {
        dynamicRepo.save(toDynamicEntity(event, de));
    }

    private StaticEnrichedEvent toStaticEntity(AdEvent event, StaticEnrichment se) {
        StaticEnrichedEvent.StaticEnrichedEventBuilder b = StaticEnrichedEvent.builder()
                .eventId(event.getEventId())
                .eventType(event.getEventType())
                .userId(event.getUserId())
                .timestamp(event.getTimestamp())
                .sourceIp(event.getSourceIp())
                .domain(event.getDomain());
        if (se != null) {
            b.department(se.getDepartment())
             .role(se.getRole())
             .site(se.getSite())
             .managerEmail(se.getManagerEmail());
        }
        return b.build();
    }

    private DynamicEnrichedEvent toDynamicEntity(AdEvent event, DynamicEnrichment de) {
        DynamicEnrichedEvent.DynamicEnrichedEventBuilder b = DynamicEnrichedEvent.builder()
                .eventId(event.getEventId())
                .eventType(event.getEventType())
                .userId(event.getUserId())
                .timestamp(event.getTimestamp())
                .sourceIp(event.getSourceIp())
                .domain(event.getDomain());
        if (de != null) {
            b.riskScore(de.getRiskScore())
             .sessionActive(de.getSessionActive())
             .policyViolation(de.getPolicyViolation())
             .lastSeenMinutesAgo(de.getLastSeenMinutesAgo());
        }
        return b.build();
    }
}
