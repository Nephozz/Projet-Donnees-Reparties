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

	public static void startJob (MapReduce mr, int format, String fname) {
		Project project = new Project();
		try {
			List<Integer> ports = project.readConfigFile().keySet().stream().collect(Collectors.toList());
			List<String> hosts = project.readConfigFile().values().stream().collect(Collectors.toList());			
		
			for (int i = 0; i < ports.size(); i++) {
				WorkerImpl worker = (WorkerImpl) Naming.lookup("rmi://" + hosts.get(i) + ":" + ports.get(i) + "/Worker");
				worker.runMap(mr, new FileReaderWriter(fname), new NetworkReaderWriter());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
