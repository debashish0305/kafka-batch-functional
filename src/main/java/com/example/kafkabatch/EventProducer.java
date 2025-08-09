package com.example.kafkabatch;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Component;

@Component
public class EventProducer {

    @Autowired
    private StreamBridge streamBridge;

    public void sendEvent(MyEvent event) {
        streamBridge.send("output-out-0", event);
    }
}