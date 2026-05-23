package com.example.adevents.dto;

import com.example.adevents.entity.DynamicEnrichedEvent;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

@Schema(name = "DynamicEnrichedEventResponse",
        description = "An AD event enriched with live runtime data (risk, session, policy).")
public record DynamicEnrichedEventResponse(
        @Schema(description = "Primary key of the enriched row", example = "42")
        Long id,

        @Schema(description = "Originating AD event id", example = "EVT-1001")
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

        @Schema(description = "Current risk score from the dynamic source (0-100)", example = "72")
        Integer riskScore,

        @Schema(description = "Whether the user has an active session right now",
                example = "true")
        Boolean sessionActive,

        @Schema(description = "Whether the event violates an active policy",
                example = "false")
        Boolean policyViolation,

        @Schema(description = "Minutes since the user was last seen", example = "5")
        Integer lastSeenMinutesAgo,

        @Schema(description = "When this enriched row was persisted (UTC)",
                example = "2024-01-15T10:30:05Z")
        Instant createdAt
) {
    public static DynamicEnrichedEventResponse fromEntity(DynamicEnrichedEvent e) {
        return new DynamicEnrichedEventResponse(
                e.getId(), e.getEventId(), e.getEventType(), e.getUserId(),
                e.getTimestamp(), e.getSourceIp(), e.getDomain(),
                e.getRiskScore(), e.getSessionActive(), e.getPolicyViolation(),
                e.getLastSeenMinutesAgo(), e.getCreatedAt()
        );
    }
}
