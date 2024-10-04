package in.sinsuren.kaf.sample;


import java.util.concurrent.ConcurrentHashMap;

public class OffsetManager {
    private final ConcurrentHashMap<Integer, Long> partitionOffsets = new ConcurrentHashMap<>();

    public OffsetManager(int partitionCount) {
        for (int i = 0; i < partitionCount; i++) {
            partitionOffsets.put(i, 0L);  // Start with offset 0 for each partition
        }
    }

    public long getOffset(int partition) {
        return partitionOffsets.get(partition);
    }

    public void incrementOffset(int partition) {
        partitionOffsets.computeIfPresent(partition, (k, v) -> v + 1);
    }
}
