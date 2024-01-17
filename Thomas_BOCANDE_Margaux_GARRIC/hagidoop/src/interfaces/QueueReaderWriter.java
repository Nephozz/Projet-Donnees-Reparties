package interfaces;

import java.util.concurrent.LinkedBlockingQueue;

public class QueueReaderWriter implements ReaderWriter {
    private LinkedBlockingQueue<KV> queue;

    public QueueReaderWriter() {
        this.queue = new LinkedBlockingQueue<KV>();
    }

    public KV read() {
        try {
            return this.queue.poll();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public void write(KV kv) {
        try {
            this.queue.offer(kv);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
