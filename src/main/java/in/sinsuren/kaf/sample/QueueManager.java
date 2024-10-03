package in.sinsuren.kaf.sample;

import java.io.*;
import java.nio.file.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class QueueManager {
  private static final String LOG_DIR = "queue_logs"; // Directory for log files
  private final String logFilePath;
  private final Lock lock = new ReentrantLock();

  public QueueManager(String topic) throws IOException {
    Files.createDirectories(Paths.get(LOG_DIR));
    logFilePath = LOG_DIR + File.separator + topic + ".log";
    File logFile = new File(logFilePath);
    if (!logFile.exists()) {
      logFile.createNewFile();
    }
  }

  // Append a message to the log file (write operation)
  public void append(String message) {
    lock.lock();
    try (FileWriter writer = new FileWriter(logFilePath, true)) {
      writer.write(message + "\n");
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      lock.unlock();
    }
  }

  // Read the log file from a specific offset (read operation)
  public String read(long offset) {
    lock.lock();
    try (BufferedReader reader = new BufferedReader(new FileReader(logFilePath))) {
      String line;
      long currentOffset = 0;
      while ((line = reader.readLine()) != null) {
        if (currentOffset == offset) {
          return line;
        }
        currentOffset++;
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      lock.unlock();
    }
    return null;
  }

  // Get the total number of messages (i.e., lines in the file)
  public long getOffset() {
    lock.lock();
    try (BufferedReader reader = new BufferedReader(new FileReader(logFilePath))) {
      return reader.lines().count();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      lock.unlock();
    }
    return -1;
  }
}
