package daemon;

import interfaces.NetworkReaderWriter;
import interfaces.QueueReaderWriter;
import interfaces.FileReaderWriter;
import interfaces.MapReduce;

import java.util.Map;

import config.Project;

public class JobLauncher {

	public static void startJob (MapReduce mr, int format, String fname) {
		QueueReaderWriter queue = new QueueReaderWriter();

		try {
			NetworkReaderWriter reader = new NetworkReaderWriter();
			reader.openSocket();
			
			for (Map.Entry<Integer,String> e: Project.servers.entrySet()) {
				// à multi threader
				ThreadedJob tj = new ThreadedJob(mr, fname, e);
				tj.start();
			}

			for (int i = 0; i < Project.servers.size(); i++) {
				ThreadedReader tr = new ThreadedReader(reader.accept(), queue);
				tr.start();
			}


			reader.closeSocket();
		} catch (Exception e) {
			e.printStackTrace();
		}

		FileReaderWriter writer = new FileReaderWriter(fname);
		writer.open("w");

		mr.reduce(queue, writer);
	}
}
