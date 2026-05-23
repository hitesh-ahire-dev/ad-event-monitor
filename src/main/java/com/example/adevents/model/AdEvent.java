package com.example.adevents.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/** Raw AD event payload coming off the {@code ad-events} Kafka topic. */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class AdEvent {
    private String eventId;
    private String eventType;
    private String userId;
    private Instant timestamp;
    private String sourceIp;
    private String domain;
}
