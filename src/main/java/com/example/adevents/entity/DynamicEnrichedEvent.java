package com.example.adevents.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "dynamic_enriched_events")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DynamicEnrichedEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false)
    private String eventId;

    @Column(name = "event_type")
    private String eventType;

    @Column(name = "user_id")
    private String userId;

    @Column(name = "event_timestamp")
    private Instant timestamp;

    @Column(name = "source_ip")
    private String sourceIp;

    @Column(name = "domain")
    private String domain;

    @Column(name = "risk_score")
    private Integer riskScore;

    @Column(name = "session_active")
    private Boolean sessionActive;

    @Column(name = "policy_violation")
    private Boolean policyViolation;

    @Column(name = "last_seen_minutes_ago")
    private Integer lastSeenMinutesAgo;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @PrePersist
    void prePersist() {
        if (createdAt == null) createdAt = Instant.now();
    }
}
