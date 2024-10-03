package in.sinsuren.kaf.sample;

import java.util.List;
import java.util.ArrayList;

import java.util.List;
import java.util.ArrayList;

public class Consumer {
    private final QueueManager queueManager;
    private long currentOffset;
    private final int partition;
    private final OffsetManager offsetManager;

    public Consumer(QueueManager queueManager, String consumerId, String topic, int partition) {
        this.queueManager = queueManager;
        this.partition = partition;
        this.offsetManager = new OffsetManager(consumerId, topic, partition);
        this.currentOffset = offsetManager.loadOffset();  // Load persisted offset
    }

    // Consume a batch of messages
    public List<String> consumeBatch(int batchSize) {
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            String message = queueManager.read(currentOffset, partition);
            if (message != null) {
                messages.add(message);
                currentOffset++;
                acknowledge();  // Acknowledge after reading
            } else {
                break;  // No more messages to consume
            }
        }
        if (messages.isEmpty()) {
            System.out.println("No new messages to consume.");
        } else {
            System.out.println("Consumed " + messages.size() + " messages from partition " + partition);
        }
        return messages;
    }

    // Acknowledge the message by saving the current offset
    private void acknowledge() {
        offsetManager.saveOffset(currentOffset);
        System.out.println("Offset updated to " + currentOffset);
    }

    public void consumeWithRetry(int retries, int retryIntervalMs) {
        int attempt = 0;
        while (attempt < retries) {
            try {
                List<String> messages = consumeBatch(1); // Batch size of 10
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
