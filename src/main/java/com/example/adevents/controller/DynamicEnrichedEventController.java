package com.example.adevents.controller;

import com.example.adevents.dto.DynamicEnrichedEventResponse;
import com.example.adevents.dto.ErrorResponse;
import com.example.adevents.service.DynamicEnrichedEventQueryService;
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
@RequestMapping(value = "/api/v1/dynamic-events", produces = MediaType.APPLICATION_JSON_VALUE)
@Tag(name = "Dynamic Enriched Events",
        description = "Read-only access to AD events enriched with live runtime data.")
public class DynamicEnrichedEventController {

    private final DynamicEnrichedEventQueryService service;

    public DynamicEnrichedEventController(DynamicEnrichedEventQueryService service) {
        this.service = service;
    }

    @Operation(
            summary = "List all dynamic-enriched events",
            description = "Returns every row in the dynamic_enriched_events table.")
    @ApiResponse(responseCode = "200",
            description = "List of dynamic-enriched events (possibly empty).",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = DynamicEnrichedEventResponse.class)))
    @GetMapping
    public ResponseEntity<List<DynamicEnrichedEventResponse>> getAll() {
        return ResponseEntity.ok(service.getAll());
    }

    @Operation(
            summary = "Get the most recent dynamic-enriched event for an eventId",
            description = "Looks up the latest row in dynamic_enriched_events for the supplied eventId.")
    @ApiResponse(responseCode = "200",
            description = "Matching dynamic-enriched event.",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = DynamicEnrichedEventResponse.class)))
    @ApiResponse(responseCode = "404",
            description = "No dynamic-enriched event exists for the given eventId.",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class)))
    @GetMapping("/{eventId}")
    public ResponseEntity<DynamicEnrichedEventResponse> getByEventId(@PathVariable String eventId) {
        return ResponseEntity.ok(service.getByEventId(eventId));
    }
}
