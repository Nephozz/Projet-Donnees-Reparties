package hdfs;

import java.io.*;
import java.net.*;

//rajouter des méthodes read, write, delete

public class HdfsServer {
    public static void main (String args[]) {
		try {
			ServerSocket serverSocket = new ServerSocket(8080);
		    while (true) {
			    Socket clientSocket = serverSocket.accept();
                ThreadServer thread = new ThreadServer(clientSocket);
                thread.start();
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
}
