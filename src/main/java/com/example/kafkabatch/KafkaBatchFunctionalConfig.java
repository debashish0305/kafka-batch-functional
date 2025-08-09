package com.example.kafkabatch;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.function.Consumer;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.beans.factory.annotation.Autowired;

@Configuration
public class KafkaBatchFunctionalConfig {

    @Bean
    public Consumer<List<MyEvent>> input() {
        return events -> {
            System.out.println("Received batch of " + events.size() + " events:");
            events.forEach(e -> System.out.println("Event: " + e.getId() + " - " + e.getName()));
        };
    }

    @Autowired
    private StreamBridge streamBridge;

    public void sendEvent(MyEvent event) {
        streamBridge.send("output-out-0", event);
    }
}