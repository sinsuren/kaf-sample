package in.sinsuren.kaf.sample;

public class Producer {
    private final QueueManager queueManager;

    public Producer(QueueManager queueManager) {
        this.queueManager = queueManager;
    }

    public void send(String message) {
        queueManager.append(message);
        System.out.println("Message produced: " + message);
    }
}
