package com.example.adevents.repository;

import com.example.adevents.entity.DynamicEnrichedEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface DynamicEnrichedEventRepository extends JpaRepository<DynamicEnrichedEvent, Long> {

    List<DynamicEnrichedEvent> findAllByEventId(String eventId);

    Optional<DynamicEnrichedEvent> findTopByEventIdOrderByIdDesc(String eventId);

    default Optional<DynamicEnrichedEvent> findByEventId(String eventId) {
        return findTopByEventIdOrderByIdDesc(eventId);
    }
}
