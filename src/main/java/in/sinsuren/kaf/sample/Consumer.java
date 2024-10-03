package in.sinsuren.kaf.sample;

import java.util.List;
import java.util.ArrayList;

public class Consumer {
    private final QueueManager queueManager;
    private long currentOffset = 0;
    private final int partition;

    public Consumer(QueueManager queueManager, int partition) {
        this.queueManager = queueManager;
        this.partition = partition;
    }

    public List<String> consumeBatch(int batchSize) {
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            String message = queueManager.read(currentOffset, partition);
            if (message != null) {
                messages.add(message);
                currentOffset++;
            } else {
                break; // No more messages to consume
            }
        }
        if (messages.isEmpty()) {
            System.out.println("No new messages to consume.");
        } else {
            System.out.println("Consumed " + messages.size() + " messages from partition " + partition);
        }
        return messages;
    }

    public void consumeWithRetry(int retries, int retryIntervalMs) {
        int attempt = 0;
        while (attempt < retries) {
            try {
                List<String> messages = consumeBatch(10); // Batch size of 10
                if (!messages.isEmpty()) {
                    break;
                }
            } catch (Exception e) {
                attempt++;
                System.out.println("Consumption failed. Retrying... Attempt: " + attempt);
                try {
                    Thread.sleep(retryIntervalMs);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public long getCurrentOffset() {
        return currentOffset;
    }
}
