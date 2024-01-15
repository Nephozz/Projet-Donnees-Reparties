package daemon;

import java.io.IOException;
import java.rmi.Naming;

import interfaces.FileReaderWriter;
import interfaces.MapReduce;
import interfaces.NetworkReaderWriter;
import java.util.List;
import java.util.stream.Collectors;

import config.Project;

public class JobLauncher {

	public static void startJob (MapReduce mr, int format, String fname) throws IOException {
		Project project = new Project();
		List<Integer> ports = project.readConfigFile().keySet().stream().collect(Collectors.toList());
		List<String> hosts = project.readConfigFile().values().stream().collect(Collectors.toList());

		try {
			for (int i = 0; i < ports.size(); i++) {
				WorkerImpl worker = (WorkerImpl) Naming.lookup("//" + hosts.get(i) + ":" + ports.get(i) + "/WorkerImpl");
				worker.runMap(mr, new FileReaderWriter(fname), new NetworkReaderWriter());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
