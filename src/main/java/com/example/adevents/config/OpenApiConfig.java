package com.example.adevents.config;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.springframework.context.annotation.Configuration;

@Configuration
@OpenAPIDefinition(
        info = @Info(
                title = "AD Enrichment Service API",
                version = "1.0",
                description = "Query statically and dynamically enriched AD events by eventId or retrieve all records"
        )
)
public class OpenApiConfig {
}
