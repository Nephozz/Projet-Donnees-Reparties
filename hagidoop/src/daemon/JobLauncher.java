package daemon;

import interfaces.NetworkReaderWriter;
import interfaces.MapReduce;

import java.net.Socket;
import java.util.Map;

import config.Project;

public class JobLauncher {

	public static void startJob (MapReduce mr, int format, String fname) {
		Project project = new Project();
		try {
			NetworkReaderWriter reader = new NetworkReaderWriter(new Socket());
			
			for (Map.Entry<Integer,String> e: project.servers.entrySet()) {
				// à multi threader
				ThreadedJob tj = new ThreadedJob(mr, fname, e);
				tj.start();
			}

			reader.closeSocket();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
