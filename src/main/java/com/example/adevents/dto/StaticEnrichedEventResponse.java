package com.example.adevents.dto;

import com.example.adevents.entity.StaticEnrichedEvent;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

@Schema(name = "StaticEnrichedEventResponse",
        description = "An AD event enriched with static reference data (department, role, site, manager).")
public record StaticEnrichedEventResponse(
        @Schema(description = "Primary key of the enriched row", example = "42")
        Long id,

        @Schema(description = "Originating AD event id (lookup key for both enrichments)",
                example = "EVT-1001")
        String eventId,

        @Schema(description = "Type of AD event", example = "USER_LOGIN")
        String eventType,

        @Schema(description = "User the event was raised for", example = "u123")
        String userId,

        @Schema(description = "Wall-clock time the event occurred (UTC)",
                example = "2024-01-15T10:30:00Z")
        Instant timestamp,

        @Schema(description = "Source IP that triggered the event", example = "192.168.1.10")
        String sourceIp,

        @Schema(description = "AD domain", example = "corp.example.com")
        String domain,

        @Schema(description = "Department from static enrichment", example = "Engineering")
        String department,

        @Schema(description = "Role from static enrichment", example = "Developer")
        String role,

        @Schema(description = "Site / office location", example = "Mumbai")
        String site,

        @Schema(description = "Manager email from static enrichment",
                example = "manager@corp.com")
        String managerEmail,

        @Schema(description = "When this enriched row was persisted (UTC)",
                example = "2024-01-15T10:30:05Z")
        Instant createdAt
) {
    public static StaticEnrichedEventResponse fromEntity(StaticEnrichedEvent e) {
        return new StaticEnrichedEventResponse(
                e.getId(), e.getEventId(), e.getEventType(), e.getUserId(),
                e.getTimestamp(), e.getSourceIp(), e.getDomain(),
                e.getDepartment(), e.getRole(), e.getSite(), e.getManagerEmail(),
                e.getCreatedAt()
        );
    }
}
