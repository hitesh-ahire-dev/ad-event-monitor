package com.example.adevents.producer;

import com.example.adevents.model.AdEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.time.Instant;
import java.util.List;
import java.util.Properties;

/**
 * Generates sample data on the {@code ad-events} topic against a real,
 * already-running Kafka broker (default {@code localhost:9092}).
 *
 * <p><b>How to run</b> (gated so it does not run on every build):
 * <pre>
 * mvn -f pom.xml test \
 *     -Dtest=AdEventProducerIT \
 *     -Dkafka.publish=true \
 *     -Dkafka.bootstrap=localhost:9092 \
 *     -Dkafka.topic=ad-events \
 *     -Dkafka.count=10
 * </pre>
 *
 * <p>Or right-click → Run in IntelliJ after editing the run config to add
 * the VM option {@code -Dkafka.publish=true}.
 */
@EnabledIfSystemProperty(named = "kafka.publish", matches = "true")
class AdEventProducerIT {

    @Test
    void publishSampleEvents() throws Exception {
        String bootstrap = System.getProperty("kafka.bootstrap", "localhost:9092");
        String topic = System.getProperty("kafka.topic", "ad-events");
        int count = Integer.parseInt(System.getProperty("kafka.count", "10"));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "ad-events-test-producer");

        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        List<String> eventTypes = List.of("USER_LOGIN", "USER_LOGOUT", "PASSWORD_CHANGE",
                "GROUP_MODIFY", "ACCOUNT_LOCKED");
        List<String> domains = List.of("corp.example.com", "eu.example.com", "apac.example.com");
        List<String> sampleIds = List.of("EVT-1001", "EVT-1002", "EVT-9999"); // first two have enrichment

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                String eventId = sampleIds.get(i % sampleIds.size());
                AdEvent event = AdEvent.builder()
                        .eventId(eventId)
                        .eventType(eventTypes.get(i % eventTypes.size()))
                        .userId("u" + (100 + i))
                        .timestamp(Instant.now())
                        .sourceIp("192.168.1." + (10 + (i % 200)))
                        .domain(domains.get(i % domains.size()))
                        .build();

                String payload = mapper.writeValueAsString(event);
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(topic, eventId, payload);

                RecordMetadata md = producer.send(record).get();
                System.out.printf("Sent eventId=%s -> %s-%d@%d%n",
                        eventId, md.topic(), md.partition(), md.offset());
            }
            producer.flush();
        }

        System.out.printf("Published %d events to topic '%s' on %s%n", count, topic, bootstrap);
    }
}
