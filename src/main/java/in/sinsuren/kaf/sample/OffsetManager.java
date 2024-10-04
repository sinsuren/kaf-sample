package in.sinsuren.kaf.sample;


import java.io.*;
import java.util.concurrent.ConcurrentHashMap;

public class OffsetManager {
    private final ConcurrentHashMap<Integer, Long> partitionOffsets = new ConcurrentHashMap<>();
    private final String offsetFilePath;

    public OffsetManager(String topic, int partitionCount) throws IOException {
        this.offsetFilePath = "offsets/" + topic + "-offsets.txt";
        initialize(partitionCount);
    }

    private void initialize(int partitionCount) throws IOException {
        File file = new File(offsetFilePath);
        if (!file.exists()) {
            file.getParentFile().mkdirs();
            file.createNewFile();
            for (int i = 0; i < partitionCount; i++) {
                partitionOffsets.put(i, 0L);
            }
            saveOffsets();
        } else {
            loadOffsets();
        }
    }

    // Get offset for a partition
    public long getOffset(int partition) {
        return partitionOffsets.getOrDefault(partition, 0L);
    }

    // Increment offset for a partition
    public void incrementOffset(int partition) {
        partitionOffsets.computeIfPresent(partition, (k, v) -> v + 1);
        try {
            saveOffsets();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Save offsets to file
    private void saveOffsets() throws IOException {
        try (FileWriter writer = new FileWriter(offsetFilePath)) {
            for (int partition : partitionOffsets.keySet()) {
                writer.write(partition + ":" + partitionOffsets.get(partition) + "\n");
            }
        }
    }

    // Load offsets from file
    private void loadOffsets() throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(offsetFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(":");
                int partition = Integer.parseInt(parts[0]);
                long offset = Long.parseLong(parts[1]);
                partitionOffsets.put(partition, offset);
            }
        }
    }
}
