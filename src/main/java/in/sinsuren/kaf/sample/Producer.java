package in.sinsuren.kaf.sample;

import java.util.concurrent.atomic.AtomicInteger;

public class Producer {
  private final QueueManager queueManager;
  private final AtomicInteger partitionCounter = new AtomicInteger(0);

  public Producer(QueueManager queueManager) {
    this.queueManager = queueManager;
  }

  // Send message without key (Round-robin partitioning)
  public void send(String message) {
    int partition = partitionCounter.getAndIncrement() % queueManager.getPartitionCount();
    queueManager.append(message, partition);
    System.out.println("Message produced to partition " + partition + ": " + message);
  }

  // Send message with key (Hash-based partitioning)
  public void send(String key, String message) {
    int partition = getPartitionForKey(key);
    queueManager.append(message, partition);
    System.out.println(
        "Message produced to partition " + partition + " with key: " + key + " - " + message);
  }

  // Compute the partition using a hash of the key
  private int getPartitionForKey(String key) {
    return Math.abs(key.hashCode()) % queueManager.getPartitionCount();
  }
}
