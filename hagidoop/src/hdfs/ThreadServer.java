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

            ObjectOutputStream outputStream = new ObjectOutputStream(clienSocket.getOutputStream());
            BufferedReader bis = new BufferedReader(new InputStreamReader(clienSocket.getInputStream()));

            String request = bis.readLine();

            if (request.startsWith("DELETE")) {
                String[] tokens = request.split(" ");
                String fname = tokens[1];
                // Supprimer le fichier fname
                String response = fname + " DELETED";
                outputStream.writeObject(response);
            } else if (request.startsWith("WRITE")) {
                String[] tokens = request.split(" ");
                String fragment = tokens[1];
                String response = fragment + " WRITTEN";
                outputStream.writeObject(response);
            } else if (request.startsWith("READ")) {
                String[] tokens = request.split(" ");
                String fname = tokens[1];
                // Lire le fichier fname
                String content = "Readind file " + fname + " ...";
                outputStream.writeObject(content);
            } else {
                System.out.println("Unknown request: " + request);
            }

            clienSocket.close();
            inputStream.close();
            outputStream.close();
		} catch (IOException ex) {
            ex.printStackTrace();
        }
	}
}
