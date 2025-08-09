package com.example.kafkadlq;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.function.Consumer;

@Configuration
public class KafkaFunctions {

    // Main consumer: processes one record at a time (per-record)
    @Bean
    public Consumer<MyEvent> myConsumer() {
        return event -> {
            System.out.println("[myConsumer] Received event: " + event);
            // Simulate processing error for specific name
            if (event != null && "FAIL".equalsIgnoreCase(event.getName())) {
                throw new RuntimeException("Processing failed for event id=" + event.getId());
            }
            // normal processing logic here
        };
    }

    // DLQ consumer - handles failed messages (from DLQ topic)
    @Bean
    public Consumer<Object> myDlqConsumer() {
        return payload -> {
            System.err.println("[myDlqConsumer] DLQ received payload: " + payload);
            // Implement alerting, persistence or retry logic here
        };
    }

    @Autowired
    private StreamBridge streamBridge;

    // Example helper to send events programmatically
    public void sendEvent(MyEvent event) {
        streamBridge.send("myProducer", event);
    }
}
