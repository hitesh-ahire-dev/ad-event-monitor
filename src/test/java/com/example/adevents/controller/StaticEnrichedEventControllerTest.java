package com.example.adevents.controller;

import com.example.adevents.dto.StaticEnrichedEventResponse;
import com.example.adevents.exception.EventNotFoundException;
import com.example.adevents.exception.GlobalExceptionHandler;
import com.example.adevents.service.StaticEnrichedEventQueryService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class StaticEnrichedEventControllerTest {

    private StaticEnrichedEventQueryService service;
    private MockMvc mvc;
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @BeforeEach
    void setup() {
        service = mock(StaticEnrichedEventQueryService.class);
        mvc = MockMvcBuilders
                .standaloneSetup(new StaticEnrichedEventController(service))
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    private StaticEnrichedEventResponse sample(String id) {
        return new StaticEnrichedEventResponse(
                1L, id, "USER_LOGIN", "u123",
                Instant.parse("2024-01-15T10:30:00Z"),
                "192.168.1.10", "corp.example.com",
                "Engineering", "Developer", "Mumbai", "manager@corp.com",
                Instant.parse("2024-01-15T10:30:05Z"));
    }

    @Test
    void getAllReturns200AndList() throws Exception {
        when(service.getAll()).thenReturn(List.of(sample("EVT-1001"), sample("EVT-1002")));

        mvc.perform(get("/api/v1/static-events").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].eventId").value("EVT-1001"));
    }

    @Test
    void getByIdReturns200AndFields() throws Exception {
        when(service.getByEventId("EVT-1001")).thenReturn(sample("EVT-1001"));

        mvc.perform(get("/api/v1/static-events/{id}", "EVT-1001"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.eventId").value("EVT-1001"))
                .andExpect(jsonPath("$.department").value("Engineering"))
                .andExpect(jsonPath("$.role").value("Developer"));
    }

    @Test
    void getByIdMissingReturns404WithErrorBody() throws Exception {
        when(service.getByEventId(eq("UNKNOWN")))
                .thenThrow(new EventNotFoundException(
                        "Static enriched event not found for eventId: UNKNOWN"));

        mvc.perform(get("/api/v1/static-events/{id}", "UNKNOWN"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error")
                        .value("Static enriched event not found for eventId: UNKNOWN"))
                .andExpect(jsonPath("$.timestamp").exists());
    }
}
