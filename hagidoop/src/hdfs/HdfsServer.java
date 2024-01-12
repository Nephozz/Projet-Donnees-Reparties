package hdfs;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

//rajouter des méthodes read, write, delete

public class HdfsServer {
	
    private Socket client;
    private int id;
    private int port;

    private HdfsServer(Socket s, int port) {
        this.client = s;
        this.port = port;
    }
	
    public static void main (String args[]) {
		try {
			ServerSocket ss = new ServerSocket(8080);
		    while (true) {
			    Socket cs = ss.accept();
                ThreadServer thread = new ThreadServer(cs);
                thread.start();
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
}

public class ThreadServer implements Thread {

    private Socket s;
    private int num;

    private TreadServer(Socket s) {
        this.s = s;
    }

    public void run () {
		try {
            Socket clienSocket = this.client;

            ObjectInputStream inputStream = new ObjectInputStream(clienSocket.getInputStream());
            ObjectOutputStream outputStream = new ObjectOutputStream(clienSocket.getOutputStream());

            Integer[] tab = (Integer[]) inputStream.readObject();

            this.id = tab[0];
            int request = tab[1];

            if (request == 0) {
                int bufferSize = tab[3];
                byte[] buffer = new byte[bufferSize];
                Integer bytesRead = in.read(buffer);
                if (bytesRead>0) {
                    this.whriteFile(buffer);
                }
            }
            if (request == 1) {
                
            }

            clienSocket.close();
            inputStream.close();
            outputStream.close();
		} catch (IOException | ClassNotFoundException ex) {
            ex.printStackTrace();
        }
	}
}
