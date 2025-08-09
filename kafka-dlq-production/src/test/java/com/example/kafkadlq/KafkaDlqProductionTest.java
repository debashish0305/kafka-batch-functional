package com.example.kafkadlq;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Instant;

@SpringBootTest
public class KafkaDlqProductionTest {

    @Autowired
    private EventProducer producer;

    @Test
    public void testProduceAndDlq() throws InterruptedException {
        // send some good messages
        for (int i = 1; i <= 5; i++) {
            producer.send(new MyEvent("id-" + i, "NAME-" + i, Instant.now().toEpochMilli()));
        }
        // send one failing message
        producer.send(new MyEvent("id-fail", "FAIL", Instant.now().toEpochMilli()));

        // wait to let consumer process and DLQ route
        Thread.sleep(5000);
    }
}
