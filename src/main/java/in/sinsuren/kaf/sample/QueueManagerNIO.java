package in.sinsuren.kaf.sample;

import java.io.File;
import java.io.IOException;
import java.nio.channels.*;
import java.nio.file.*;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class QueueManagerNIO {
    private static final String LOG_DIR = "queue_logs";  // Directory for log files
    private final String topic;
    private final int partitionCount;
    private final long segmentSizeLimit;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public QueueManagerNIO(String topic, int partitionCount, long segmentSizeLimit) throws IOException {
        this.topic = topic;
        this.partitionCount = partitionCount;
        this.segmentSizeLimit = segmentSizeLimit;
        initializePartitions();
    }

    private void initializePartitions() throws IOException {
        Files.createDirectories(Paths.get(LOG_DIR));
        for (int i = 0; i < partitionCount; i++) {
            String partitionDir = LOG_DIR + File.separator + topic + "-partition-" + i;
            Files.createDirectories(Paths.get(partitionDir));
        }
    }

    // Append message to a partition log asynchronously
    public void appendAsync(String message, int partition) {
        lock.writeLock().lock();
        try {
            String partitionDir = LOG_DIR + File.separator + topic + "-partition-" + partition;
            Path logFilePath = Paths.get(partitionDir, "segment-" + System.currentTimeMillis() + ".log");

            try (AsynchronousFileChannel asyncFileChannel = AsynchronousFileChannel.open(logFilePath, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
                ByteBuffer buffer = ByteBuffer.wrap((message + "\n").getBytes());
                asyncFileChannel.write(buffer, asyncFileChannel.size());
            } catch (IOException e) {
                e.printStackTrace();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}
