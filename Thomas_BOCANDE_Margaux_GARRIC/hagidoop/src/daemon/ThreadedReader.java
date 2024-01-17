package daemon;

import interfaces.KV;
import interfaces.NetworkReaderWriter;
import interfaces.QueueReaderWriter;

// Classe permetant de lire de manière concurante les résultats des maps
public class ThreadedReader extends Thread {
    private NetworkReaderWriter reader;
    private QueueReaderWriter queue;

    public ThreadedReader(NetworkReaderWriter reader, QueueReaderWriter queue) {
        super();
        this.reader = reader;
        this.queue = queue;
    }

    public void run() {
        KV kv;

        do {
            kv = this.reader.read();
            this.queue.write(kv);
        } while (kv != null);
    }
}
