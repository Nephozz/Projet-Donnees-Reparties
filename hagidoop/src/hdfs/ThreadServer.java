package hdfs;

import java.net.*;
import java.io.*;

public class ThreadServer extends Thread {

    private Socket client;

    public ThreadServer(Socket s) {
        this.client = s;
    }

    public void run () {
		try {
            Socket clienSocket = this.client;

            ObjectOutputStream outputStream = new ObjectOutputStream(clienSocket.getOutputStream());
            BufferedReader bufferedInputStream = new BufferedReader(new InputStreamReader(clienSocket.getInputStream()));

            String request = bufferedInputStream.readLine();

            if (request.startsWith("DELETE")) {
                String[] tokens = request.split(" ");
                String fname = tokens[1];
                // Supprimer le fichier fname
                String response = fname + " DELETED";
                outputStream.writeObject(response);
            } else if (request.startsWith("WRITE")) {
                String[] tokens = request.split(" ");
                int fmt = Integer.parseInt(tokens[1]);
                String fragment = tokens[2];
                if (fmt == 0) {
                    // Ecrire le fragment sous le format txt
                } else if (fmt == 1) {
                    // Ecrire le fragment sous le format kv
                } else {
                    System.out.println("Unknown format: " + fmt);
                }
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
            bufferedInputStream.close();
            outputStream.close();
		} catch (IOException ex) {
            ex.printStackTrace();
        }
	}
}
