package in.sinsuren.kaf.sample;

import java.util.List;
import java.util.ArrayList;

import java.util.List;
import java.util.ArrayList;

public class Consumer {
    private final QueueManager queueManager;
    private final OffsetManager offsetManager;
    private final int partition;

    public Consumer(QueueManager queueManager, OffsetManager offsetManager, int partition) {
        this.queueManager = queueManager;
        this.offsetManager = offsetManager;
        this.partition = partition;
    }

    public void consume() {
        long offset = offsetManager.getOffset(partition);
        String message = queueManager.read(offset, partition);
        if (message != null) {
            System.out.println("Message consumed: " + message);
            offsetManager.incrementOffset(partition);
        } else {
            System.out.println("No new messages.");
        }
    }
}
