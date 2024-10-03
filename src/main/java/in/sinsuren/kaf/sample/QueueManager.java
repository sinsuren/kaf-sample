package in.sinsuren.kaf.sample;

import java.io.*;
import java.nio.file.*;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class QueueManager {
  private static final String LOG_DIR = "queue_logs"; // Directory for log files
  private static final long MAX_LOG_SIZE = 1024 * 1024; // 1 MB max size for log files
  private static final long RETENTION_PERIOD_MS =
      Duration.ofHours(24).toMillis(); // 24 hours retention

  private final String topic;

  public int getPartitionCount() {
    return partitionCount;
  }

  public final int partitionCount;
  private final Lock lock = new ReentrantLock();

  public QueueManager(String topic, int partitionCount) throws IOException {
    this.topic = topic;
    this.partitionCount = partitionCount;
    Files.createDirectories(Paths.get(LOG_DIR));
    for (int i = 0; i < partitionCount; i++) {
      createLogFile(i);
    }
  }

  private String getPartitionLogFile(int partition) {
    return LOG_DIR + File.separator + topic + "_partition_" + partition + ".log";
  }

  private void createLogFile(int partition) throws IOException {
    String logFilePath = getPartitionLogFile(partition);
    File logFile = new File(logFilePath);
    if (!logFile.exists()) {
      logFile.createNewFile();
    }
  }

  public void append(String message, int partition) {
    lock.lock();
    try {
      String logFilePath = getPartitionLogFile(partition);
      File logFile = new File(logFilePath);

      // Check if file size exceeds limit
      if (logFile.length() > MAX_LOG_SIZE) {
        rotateLogFile(partition);
      }

      try (FileWriter writer = new FileWriter(logFile, true)) {
        writer.write(message + "\n");
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      lock.unlock();
    }
  }

  public String read(long offset, int partition) {
    lock.lock();
    try {
      String logFilePath = getPartitionLogFile(partition);
      try (BufferedReader reader = new BufferedReader(new FileReader(logFilePath))) {
        String line;
        long currentOffset = 0;
        while ((line = reader.readLine()) != null) {
          if (currentOffset == offset) {
            return line;
          }
          currentOffset++;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      lock.unlock();
    }
    return null;
  }

  public long getOffset(int partition) {
    lock.lock();
    try {
      String logFilePath = getPartitionLogFile(partition);
      try (BufferedReader reader = new BufferedReader(new FileReader(logFilePath))) {
        return reader.lines().count();
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      lock.unlock();
    }
    return -1;
  }

  private void rotateLogFile(int partition) throws IOException {
    String logFilePath = getPartitionLogFile(partition);
    File oldLogFile = new File(logFilePath);
    String backupLogFile = logFilePath + "." + Instant.now().toEpochMilli();
    Files.move(oldLogFile.toPath(), Paths.get(backupLogFile), StandardCopyOption.REPLACE_EXISTING);
    createLogFile(partition);
  }

  // Cleanup old log files based on retention policy
  public void cleanup() {
    File logDir = new File(LOG_DIR);
    File[] files = logDir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isFile() && file.getName().contains(topic)) {
          long lastModified = file.lastModified();
          long age = System.currentTimeMillis() - lastModified;
          if (age > RETENTION_PERIOD_MS) {
            file.delete();
          }
        }
      }
    }
  }
}
