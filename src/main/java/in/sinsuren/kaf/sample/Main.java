package in.sinsuren.kaf.sample;

import java.io.IOException;

public class Main {
  public static void main(String[] args) throws IOException {
    // Create queue manager with 3 partitions
    QueueManager queueManager = new QueueManager("my_topic", 3);

    // Create producer
    Producer producer = new Producer(queueManager);

    // Produce some messages to different partitions
    producer.send("Message 1");
    producer.send("Message 2");
    producer.send("Message 3");
    producer.send("Message 4");

    // Create consumers for each partition
    Consumer consumer1 = new Consumer(queueManager, 0);
    Consumer consumer2 = new Consumer(queueManager, 1);
    Consumer consumer3 = new Consumer(queueManager, 2);

    // Consume with retry logic and batch size
    consumer1.consumeWithRetry(3, 1000); // Retry 3 times with 1-second intervals
    consumer2.consumeWithRetry(3, 1000);
    consumer3.consumeWithRetry(3, 1000);

    // Simulate log cleanup based on retention policy
    queueManager.cleanup();
  }
}
