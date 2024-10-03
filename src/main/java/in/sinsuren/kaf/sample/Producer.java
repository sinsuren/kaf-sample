package in.sinsuren.kaf.sample;

import java.util.concurrent.atomic.AtomicInteger;

public class Producer {
  private final QueueManager queueManager;
  private final AtomicInteger partitionCounter = new AtomicInteger(0);

  public Producer(QueueManager queueManager) {
    this.queueManager = queueManager;
  }

  public void send(String message) {
    // Round-robin partition selection
    int partition = partitionCounter.getAndIncrement() % queueManager.partitionCount;
    queueManager.append(message, partition);
    System.out.println("Message produced to partition " + partition + ": " + message);
  }
}
