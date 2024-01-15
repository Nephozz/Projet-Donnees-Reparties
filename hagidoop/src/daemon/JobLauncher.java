package daemon;

import java.rmi.Naming;

import interfaces.FileReaderWriter;
import interfaces.MapReduce;
import interfaces.NetworkReaderWriter;

import java.util.Map;

import config.Project;

public class JobLauncher {

	public static void startJob (MapReduce mr, int format, String fname) {
		Project project = new Project();
		try {
			for (Map.Entry<Integer,String> e: project.servers.entrySet()) {
				WorkerImpl worker = (WorkerImpl) Naming.lookup("rmi://" + e.getValue() + ":" + e.getKey() + "/Worker");
				worker.runMap(mr, new FileReaderWriter(fname), new NetworkReaderWriter());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
