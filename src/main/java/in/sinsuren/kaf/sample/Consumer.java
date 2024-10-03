package in.sinsuren.kaf.sample;

public class Consumer {
    private final QueueManager queueManager;
    private long currentOffset = 0;

    public Consumer(QueueManager queueManager) {
        this.queueManager = queueManager;
    }

    public void consume() {
        String message = queueManager.read(currentOffset);
        if (message != null) {
            System.out.println("Message consumed: " + message);
            currentOffset++;
        } else {
            System.out.println("No new messages to consume.");
        }
    }

    public long getCurrentOffset() {
        return currentOffset;
    }
}
