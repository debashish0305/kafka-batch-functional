package com.example.batch;

import java.util.concurrent.Semaphore;
import org.springframework.stereotype.Component;

@Component
public class DbConcurrencyLimiter {
    private final Semaphore semaphore = new Semaphore(50); // Match max pool size

    public void acquire() throws InterruptedException {
        semaphore.acquire();
    }

    public void release() {
        semaphore.release();
    }
}