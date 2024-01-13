package hdfs;

import java.net.*;
import java.io.*;

import interfaces.FileReaderWriterImpl;
import interfaces.KV;

public class ThreadServer extends Thread {
    private Socket client;

    public ThreadServer(Socket s) {
        this.client = s;
    }

    public void run () {
		try {
            Socket clientSocket = this.client;

            ObjectOutputStream outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
            BufferedReader reader= new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

            while (true) {
                String request = reader.readLine();

                if (request.startsWith("DELETE")) {
                    String[] tokens = request.split(" ");
                    String fname = tokens[1];
                    // Supprimer le fichier fname
                    String response = fname + " DELETED";
                    outputStream.writeObject(response);
                } else if (request.startsWith("WRITE")) {
                    String[] tokens = request.split(" ");
                    String fname = tokens[1];
                    int fmt = Integer.parseInt(tokens[2]);

                    if (fmt == 0) {
                        // Ecrire le fragment sous le format txt
                        BufferedWriter writer = new BufferedWriter(new FileWriter(fname, true));
                        String fragment = tokens[3];

                        writer.write(fragment);
                        writer.close();
                    } else if (fmt == 1) {
                        // Ecrire le fragment sous le format kv
                        KV fragment = new KV(tokens[3], tokens[4]);
                        FileReaderWriterImpl file = new FileReaderWriterImpl(fname);
                        file.open("w");
                        file.write(fragment);
                        file.close();
                    } else {
                        System.out.println("Unknown format: " + fmt);
                    }
                    String response = fname + " WRITTEN";
                    outputStream.writeObject(response);
                } else if (request.startsWith("READ")) {
                    String[] tokens = request.split(" ");
                    String fname = tokens[1];

                    FileReaderWriterImpl file = new FileReaderWriterImpl(fname);
                    file.open("r");

                    String response = "Readind file " + fname + " ...";
                    outputStream.writeObject(response);
                    
                    // Lire le fichier fname
                    KV content = file.read();

                    while (content.k != null) {
                        outputStream.writeObject(content);
                        content = file.read();
                    }

                    content = null;
                    outputStream.writeObject(content);

                    String end = "END OF FILE";
                    outputStream.writeObject(end);

                    file.close();
                } else {
                    System.out.println("Unknown request: " + request);
                }
            }
		} catch (IOException ex) {
            ex.printStackTrace();
        }
	}
}
