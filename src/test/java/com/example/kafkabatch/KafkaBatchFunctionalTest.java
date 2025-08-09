package com.example.kafkabatch;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Instant;

@SpringBootTest
public class KafkaBatchFunctionalTest {

    @Autowired
    private EventProducer producer;

    @Test
    public void testProduceConsumeBatch() throws InterruptedException {
        for (int i = 1; i <= 20; i++) {
            MyEvent event = new MyEvent("id-" + i, "name-" + i, Instant.now().toEpochMilli());
            producer.sendEvent(event);
        }
        // Wait for batch consumer to process
        Thread.sleep(5000);
    }
}