package com.example.adevents.config;

import com.example.adevents.model.AdEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration.
 * <p>
 * - Uses {@link ErrorHandlingDeserializer} so that poison-pill messages
 *   (bad JSON, etc.) are turned into a {@code DeserializationException}
 *   header instead of crashing the consumer.
 * - {@link DefaultErrorHandler} with a short fixed backoff retries
 *   transient errors a couple of times then logs &amp; skips
 *   (acts as a simple "dead-letter" by skipping after retries).
 */
@Configuration
@EnableKafka
public class KafkaConfig {

    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    public KafkaConfig(ObjectMapper objectMapper, MeterRegistry meterRegistry) {
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
    }

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${app.kafka.topic:ad-events}")
    private String topic;

    @Value("${app.kafka.partitions:1}")
    private int partitions;

    @Value("${app.kafka.replicas:1}")
    private short replicas;

    /**
     * Auto-creates the {@code ad-events} topic at startup via {@code KafkaAdmin}.
     * If the topic already exists this is a no-op.
     */
    @Bean
    public NewTopic adEventsTopic() {
        return TopicBuilder.name(topic)
                .partitions(partitions)
                .replicas(replicas)
                .build();
    }

    @Bean
    public ConsumerFactory<String, AdEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Build the JsonDeserializer ourselves so it uses Spring Boot's ObjectMapper
        // (which has JavaTimeModule registered — required for Instant fields).
        JsonDeserializer<AdEvent> jsonValueDeserializer =
                new JsonDeserializer<>(AdEvent.class, objectMapper, false);
        jsonValueDeserializer.addTrustedPackages("*");

        ErrorHandlingDeserializer<String> keyDeserializer =
                new ErrorHandlingDeserializer<>(new StringDeserializer());
        ErrorHandlingDeserializer<AdEvent> valueDeserializer =
                new ErrorHandlingDeserializer<>(jsonValueDeserializer);

        DefaultKafkaConsumerFactory<String, AdEvent> factory =
                new DefaultKafkaConsumerFactory<>(props, keyDeserializer, valueDeserializer);
        // Exposes Kafka client metrics (records-lag-max, records-consumed-rate, …)
        // via Micrometer so they show up on /actuator/prometheus.
        factory.addListener(new MicrometerConsumerListener<>(meterRegistry));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, AdEvent> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, AdEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        // 2 retries with 500ms back-off, then log &amp; skip (effective "DLT-by-skip")
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(500L, 2L)));
        return factory;
    }
}
