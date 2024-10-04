package in.sinsuren.kaf.sample;

import java.util.concurrent.atomic.AtomicInteger;

public class Producer {
  private final QueueManager queueManager;

  public Producer(QueueManager queueManager) {
    this.queueManager = queueManager;
  }

  public void send(String message, int partition) {
    queueManager.append(message, partition);
    System.out.println("Message produced: " + message);
  }
}
