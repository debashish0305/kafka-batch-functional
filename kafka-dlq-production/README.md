Kafka DLQ Production Sample

This minimal project demonstrates:
- Spring Cloud Stream (functional) + Kafka binder
- Per-record consumer (Consumer<MyEvent>)
- DLQ enabled for the consumer; failed records go to 'my-dlq-topic'
- DLQ consumer to handle failed messages
- StreamBridge-based producer helper

How to run:
1. Start Kafka (localhost:9092) — e.g., using Docker
2. Build: mvn -DskipTests package
3. Run: java -jar target/kafka-dlq-production-0.0.1-SNAPSHOT.jar
4. Use EventProducer (bean) to send events, or call StreamBridge "myProducer" binding.

Notes:
- Configure Kafka bootstrap servers and topics for production via externalized config.
- Tune retry/backoff strategies and monitoring for production use.
