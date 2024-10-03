package in.sinsuren.kaf.sample;

import java.io.IOException;

public class Main {
  public static void main(String[] args) throws IOException {
    QueueManager queueManager = new QueueManager("my_topic");

    // Create producer
    Producer producer = new Producer(queueManager);

    // Produce some messages
    producer.send("Hello, World!");
    producer.send("This is a message queue.");
    producer.send("File-based Kafka implementation.");

    // Create consumer
    Consumer consumer = new Consumer(queueManager);

    // Consume messages one by one
    consumer.consume(); // Consume "Hello, World!"
    consumer.consume(); // Consume "This is a message queue."
    consumer.consume(); // Consume "File-based Kafka implementation."
    consumer.consume(); // No more messages to consume
  }
}
