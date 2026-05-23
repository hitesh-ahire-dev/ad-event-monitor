package com.example.adevents.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "static_enriched_events")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StaticEnrichedEvent {

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

    @Column(name = "department")
    private String department;

    @Column(name = "role")
    private String role;

    @Column(name = "site")
    private String site;

    @Column(name = "manager_email")
    private String managerEmail;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @PrePersist
    void prePersist() {
        if (createdAt == null) createdAt = Instant.now();
    }
}
