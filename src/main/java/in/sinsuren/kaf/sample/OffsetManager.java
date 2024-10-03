package in.sinsuren.kaf.sample;

import java.io.*;

public class OffsetManager {
    private final String offsetFilePath;

    public OffsetManager(String consumerId, String topic, int partition) {
        this.offsetFilePath = "offsets" + File.separator + consumerId + "_" + topic + "_partition_" + partition + ".offset";
        createOffsetFile();
    }

    private void createOffsetFile() {
        File offsetFile = new File(offsetFilePath);
        try {
            File directory = new File("offsets");
            if (!directory.exists()) {
                directory.mkdir();
            }
            if (!offsetFile.exists()) {
                offsetFile.createNewFile();
                saveOffset(0);  // Initialize offset to 0
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Save the current offset to the file
    public void saveOffset(long offset) {
        try (FileWriter writer = new FileWriter(offsetFilePath, false)) {
            writer.write(String.valueOf(offset));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Load the offset from the file
    public long loadOffset() {
        try (BufferedReader reader = new BufferedReader(new FileReader(offsetFilePath))) {
            String offsetStr = reader.readLine();
            return Long.parseLong(offsetStr);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;  // Default offset if the file is empty
    }
}
