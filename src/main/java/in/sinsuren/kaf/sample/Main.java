package in.sinsuren.kaf.sample;

import java.io.IOException;

public class Main {
  public static void main(String[] args) throws IOException {
    QueueManager queueManager =
        new QueueManager("my_topic", 3, 1024 * 1024); // 3 partitions, segment size of 1MB
    OffsetManager offsetManager = new OffsetManager("my_topic", 3);

    // Create producer
    Producer producer = new Producer(queueManager);

    // Produce some messages into different partitions
    producer.send("Hello, World!", 0);
    producer.send("This is a message queue.", 1);
    producer.send("File-based Kafka implementation.", 2);

    // Create consumer for partition 0
    Consumer consumer = new Consumer(queueManager, offsetManager, 0);
    Consumer consumer2 = new Consumer(queueManager, offsetManager, 1);
    Consumer consumer3 = new Consumer(queueManager, offsetManager, 2);

    // Consume messages from partition 0
    consumer.consume(); // Consume "Hello, World!"
    consumer2.consume(); // No more messages.
    consumer3.consume(); // No more messages.
  }
}
