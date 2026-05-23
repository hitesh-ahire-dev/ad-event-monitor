package com.example.adevents.service;

import com.example.adevents.dto.StaticEnrichedEventResponse;
import com.example.adevents.exception.EventNotFoundException;
import com.example.adevents.repository.StaticEnrichedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class StaticEnrichedEventQueryService {

    private final StaticEnrichedEventRepository repository;

    public StaticEnrichedEventQueryService(StaticEnrichedEventRepository repository) {
        this.repository = repository;
    }

    @Transactional(readOnly = true)
    public List<StaticEnrichedEventResponse> getAll() {
        return repository.findAll().stream()
                .map(StaticEnrichedEventResponse::fromEntity)
                .toList();
    }

    @Transactional(readOnly = true)
    public StaticEnrichedEventResponse getByEventId(String eventId) {
        return repository.findByEventId(eventId)
                .map(StaticEnrichedEventResponse::fromEntity)
                .orElseThrow(() -> new EventNotFoundException(
                        "Static enriched event not found for eventId: " + eventId));
    }
}
