package com.example.kafkadlq;

import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;

@Component
public class EventProducer {

    @Autowired
    private StreamBridge streamBridge;

    public void send(MyEvent event) {
        streamBridge.send("myProducer", event);
    }
}
