package in.sinsuren.kaf.sample;

import java.io.IOException;

public class Main {
  public static void main(String[] args) throws IOException {
    // Create queue manager with 3 partitions
    QueueManager queueManager = new QueueManager("my_topic", 3);

    // Create producer
    Producer producer = new Producer(queueManager);

    // Produce some messages with keys (for hash-based partitioning)
    producer.send("key1", "Message 1");
    producer.send("key2", "Message 2");
    producer.send("key3", "Message 3");
    producer.send("key1", "Message 4");
    producer.send("key2", "Message 5");

    // Create consumers for each partition with offset persistence
    Consumer consumer1 = new Consumer(queueManager, "consumer1", "my_topic", 0);
    Consumer consumer2 = new Consumer(queueManager, "consumer2", "my_topic", 1);
    Consumer consumer3 = new Consumer(queueManager, "consumer3", "my_topic", 2);

    // Consume messages with retry logic and batch size
    consumer1.consumeWithRetry(3, 1000); // Retry 3 times with 1-second intervals
    consumer2.consumeWithRetry(3, 1000);
    consumer3.consumeWithRetry(3, 1000);

    // Produce more messages after consuming
    producer.send("key1", "Message 6");
    producer.send("key3", "Message 7");

    // Consume again to check offset persistence
    consumer1.consumeWithRetry(3, 1000);
    consumer2.consumeWithRetry(3, 1000);
    consumer3.consumeWithRetry(3, 1000);

    // Simulate log cleanup based on retention policy
    queueManager.cleanup();
  }
}
