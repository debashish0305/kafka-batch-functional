package com.example.kafkabatch;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MyEvent {
    private String id;
    private String name;
    private long timestamp;
}