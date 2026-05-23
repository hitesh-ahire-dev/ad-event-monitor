package com.example.adevents.repository;

import com.example.adevents.entity.StaticEnrichedEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface StaticEnrichedEventRepository extends JpaRepository<StaticEnrichedEvent, Long> {

    /** All rows for an event id (history of repeated enrichments). */
    List<StaticEnrichedEvent> findAllByEventId(String eventId);

    /** Most recently inserted row for an event id, queried in DB (not in memory). */
    Optional<StaticEnrichedEvent> findTopByEventIdOrderByIdDesc(String eventId);

    /**
     * Convenience lookup as required by the API spec: returns the most
     * recent enriched row for the given event id, if any.
     */
    default Optional<StaticEnrichedEvent> findByEventId(String eventId) {
        return findTopByEventIdOrderByIdDesc(eventId);
    }
}
