package com.example.adevents.service;

import com.example.adevents.dto.DynamicEnrichedEventResponse;
import com.example.adevents.exception.EventNotFoundException;
import com.example.adevents.repository.DynamicEnrichedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class DynamicEnrichedEventQueryService {

    private final DynamicEnrichedEventRepository repository;

    public DynamicEnrichedEventQueryService(DynamicEnrichedEventRepository repository) {
        this.repository = repository;
    }

    @Transactional(readOnly = true)
    public List<DynamicEnrichedEventResponse> getAll() {
        return repository.findAll().stream()
                .map(DynamicEnrichedEventResponse::fromEntity)
                .toList();
    }

    @Transactional(readOnly = true)
    public DynamicEnrichedEventResponse getByEventId(String eventId) {
        return repository.findByEventId(eventId)
                .map(DynamicEnrichedEventResponse::fromEntity)
                .orElseThrow(() -> new EventNotFoundException(
                        "Dynamic enriched event not found for eventId: " + eventId));
    }
}
