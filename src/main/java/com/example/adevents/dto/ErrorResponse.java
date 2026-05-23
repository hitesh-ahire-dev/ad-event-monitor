package com.example.adevents.dto;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(name = "ErrorResponse", description = "Standard error payload returned by the API.")
public record ErrorResponse(
        @Schema(description = "Human-readable error message",
                example = "Static enriched event not found for eventId: EVT-9999")
        String error,

        @Schema(description = "ISO-8601 timestamp when the error was produced (UTC)",
                example = "2024-01-15T10:30:05Z")
        String timestamp
) {}
