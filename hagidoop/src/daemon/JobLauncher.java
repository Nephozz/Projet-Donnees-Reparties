package daemon;

import java.rmi.Naming;

import interfaces.MapReduce;

public class JobLauncher {

	public static void startJob (MapReduce mr, int format, String fname) {
		try {
			WorkerImpl worker = (WorkerImpl) Naming.lookup("rmi://localhost:8080/worker");

			worker.runMap(mr, null, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
