package com.example.adevents.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

/**
 * Dedicated executor used to run the two enrichments in parallel for one event.
 *
 * <p><b>Tradeoffs of going parallel:</b>
 * <ul>
 *   <li>+ Latency: static (in-memory map) and dynamic (cache / remote) run concurrently.</li>
 *   <li>- Throughput per partition: we tie up a worker thread per in-flight event;
 *       if you already get parallelism from many Kafka partitions and concurrent
 *       containers, the gain shrinks.</li>
 *   <li>- Ordering: per partition Kafka guarantees order; if you fan out to a pool
 *       and ack early you can lose that. Here we still {@code .get()} both futures
 *       before saving &amp; acking, so order is preserved.</li>
 *   <li>- Backpressure: a bounded queue + caller-runs policy is required,
 *       otherwise an unhealthy enrichment source fills heap.</li>
 * </ul>
 */
@Configuration
public class AsyncConfig {

    @Bean(name = "enrichmentExecutor")
    public Executor enrichmentExecutor() {
        ThreadPoolTaskExecutor exec = new ThreadPoolTaskExecutor();
        exec.setCorePoolSize(8);
        exec.setMaxPoolSize(16);
        exec.setQueueCapacity(500);
        exec.setThreadNamePrefix("enrich-");
        exec.initialize();
        return exec;
    }
}
