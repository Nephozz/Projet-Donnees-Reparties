package daemon;

import java.util.Map;
import java.rmi.*;

import interfaces.FileReaderWriter;
import interfaces.MapReduce;
import interfaces.NetworkReaderWriter;

public class ThreadedJob extends Thread {
    private Map.Entry<Integer, String> entry;
    private MapReduce mr;
    private String fname;

    ThreadedJob(MapReduce mr, String fname, Map.Entry<Integer, String> entry) {
        super();
        this.mr = mr;
        this.fname = fname;
        this.entry = entry;
    }

    public void run() {
        try {
            WorkerImpl worker = (WorkerImpl) Naming.lookup("rmi://" + entry.getValue() + ":" + entry.getKey() + "/Worker");
            worker.runMap(this.mr, new FileReaderWriter(this.fname), new NetworkReaderWriter());  
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
