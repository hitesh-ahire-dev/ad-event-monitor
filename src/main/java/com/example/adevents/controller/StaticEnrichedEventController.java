package com.example.adevents.controller;

import com.example.adevents.dto.ErrorResponse;
import com.example.adevents.dto.StaticEnrichedEventResponse;
import com.example.adevents.service.StaticEnrichedEventQueryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping(value = "/api/v1/static-events", produces = MediaType.APPLICATION_JSON_VALUE)
@Tag(name = "Static Enriched Events",
        description = "Read-only access to AD events enriched with static reference data.")
public class StaticEnrichedEventController {

    private final StaticEnrichedEventQueryService service;

    public StaticEnrichedEventController(StaticEnrichedEventQueryService service) {
        this.service = service;
    }

    @Operation(
            summary = "List all static-enriched events",
            description = "Returns every row in the static_enriched_events table.")
    @ApiResponse(responseCode = "200",
            description = "List of static-enriched events (possibly empty).",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = StaticEnrichedEventResponse.class)))
    @GetMapping
    public ResponseEntity<List<StaticEnrichedEventResponse>> getAll() {
        return ResponseEntity.ok(service.getAll());
    }

    @Operation(
            summary = "Get the most recent static-enriched event for an eventId",
            description = "Looks up the latest row in static_enriched_events for the supplied eventId.")
    @ApiResponse(responseCode = "200",
            description = "Matching static-enriched event.",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = StaticEnrichedEventResponse.class)))
    @ApiResponse(responseCode = "404",
            description = "No static-enriched event exists for the given eventId.",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class)))
    @GetMapping("/{eventId}")
    public ResponseEntity<StaticEnrichedEventResponse> getByEventId(@PathVariable String eventId) {
        return ResponseEntity.ok(service.getByEventId(eventId));
    }
}
