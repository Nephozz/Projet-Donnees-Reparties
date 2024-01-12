package hdfs;

import java.net.*;
import java.io.*;

public class ThreadServer extends Thread {

    private Socket client;
    private int id;

    public ThreadServer(Socket s) {
        this.client = s;
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
                Integer bytesRead = inputStream.read(buffer);
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
