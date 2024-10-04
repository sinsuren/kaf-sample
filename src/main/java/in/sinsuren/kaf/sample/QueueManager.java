package in.sinsuren.kaf.sample;

import java.io.*;
import java.nio.file.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import java.io.*;
import java.nio.file.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class QueueManager {
  private static final String LOG_DIR = "queue_logs";  // Directory for log files
  private final String topic;
  private final int partitionCount;
  private final long segmentSizeLimit;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  public QueueManager(String topic, int partitionCount, long segmentSizeLimit) throws IOException {
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

  // Append message to a partition log
  public void append(String message, int partition) {
    lock.writeLock().lock();
    try {
      String partitionDir = LOG_DIR + File.separator + topic + "-partition-" + partition;
      File[] logFiles = new File(partitionDir).listFiles((dir, name) -> name.endsWith(".log"));

      Arrays.sort(logFiles, Comparator.comparingLong(File::lastModified));
      File latestLogFile = (logFiles == null || logFiles.length == 0) ? null : logFiles[logFiles.length - 1];
      if (latestLogFile == null || latestLogFile.length() > segmentSizeLimit) {
        latestLogFile = new File(partitionDir, "segment-" + System.currentTimeMillis() + ".log");
        latestLogFile.createNewFile();
      }

      try (FileWriter writer = new FileWriter(latestLogFile, true)) {
        writer.write(message + "\n");
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      lock.writeLock().unlock();
    }
  }

  // Read message from a specific offset
  public String read(long offset, int partition) {
    lock.readLock().lock();
    try {
      String partitionDir = LOG_DIR + File.separator + topic + "-partition-" + partition;
      File[] logFiles = new File(partitionDir).listFiles((dir, name) -> name.endsWith(".log"));
      Arrays.sort(logFiles, Comparator.comparingLong(File::lastModified));

      long currentOffset = 0;
      for (File logFile : logFiles) {
        try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
          String line;
          while ((line = reader.readLine()) != null) {
            if (currentOffset == offset) {
              return line;
            }
            currentOffset++;
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      lock.readLock().unlock();
    }
    return null;
  }

  // Get latest offset in a partition
  public long getLatestOffset(int partition) {
    lock.readLock().lock();
    try {
      String partitionDir = LOG_DIR + File.separator + topic + "-partition-" + partition;
      File[] logFiles = new File(partitionDir).listFiles((dir, name) -> name.endsWith(".log"));
      Arrays.sort(logFiles, Comparator.comparingLong(File::lastModified));

      long currentOffset = 0;
      for (File logFile : logFiles) {
        try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
          currentOffset += reader.lines().count();
        }
      }
      return currentOffset;
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      lock.readLock().unlock();
    }
    return -1;
  }
}
