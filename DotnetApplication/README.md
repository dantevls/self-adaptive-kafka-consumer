# self-adaptive-kafka-consumer
.NET 8 Kafka consumer that auto-tunes fetch.min.bytes at runtime using a PI feedback controller. A background monitor measures throughput (bytes/s) and the controller adjusts fetch settings to hit a target rate. Includes benchmark mode for controlled step-increment experiments. Built for Master's research on adaptive middleware.
