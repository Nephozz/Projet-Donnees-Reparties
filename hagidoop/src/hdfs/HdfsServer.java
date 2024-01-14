package hdfs;

import java.io.*;
import java.net.*;

import interfaces.FileReaderWriterImpl;
import interfaces.KV;

//manque un séparateur sur différent thread
public class HdfsServer extends Thread {
    private Socket client;

    public HdfsServer(Socket s) {
        this.client = s;
    }

    public void run () {
		try {
            Socket clientSocket = this.client;

            BufferedReader inputStream = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter outputStream = new PrintWriter(clientSocket.getOutputStream(), true);

            //ObjectOutputStream outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
            //ObjectInputStream inputStream = new ObjectInputStream(clientSocket.getInputStream());
            //BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            //String request = reader.readLine();
            
            String request = inputStream.readLine();

            while (request != null) {

                if (request.startsWith("DELETE")) {
                    String[] tokens = request.split(" ");
                    String fname = tokens[1];
                    // Supprimer le fichier fname
                    //FileReaderWriterImpl file = new FileReaderWriterImpl(fname);
                    //file.delete();
                    String response = fname + " DELETED";
                    outputStream.write(response);
                } else if (request.startsWith("WRITE")) {
                    //System.out.println(1);
                    String[] tokens = request.split(" ");
                    String fname = tokens[1];
                    //System.out.println(fname);
                    int fmt = Integer.parseInt(tokens[2]);
                    //System.out.println(fmt);

                    if (fmt == 0) {
                        // Ecrire le fragment sous le format txt
                        String fragment = "";
                        String extract;
                        while ((extract = inputStream.readLine())!=null) {
                            fragment = fragment + extract;
                        }
                        //String fragment = inputStream.readObject().toString();
                        FileReaderWriterImpl file = new FileReaderWriterImpl("test-server.txt");
                        file.open("w");
                        String[] lines = fragment.split("\n");

                        for (String line : lines) {
                            KV kv = new KV(line, String.valueOf(file.getIndex()));
                            file.write(kv);
                        }
                        file.close();
                    } else if (fmt == 1) {
                        // Ecrire le fragment sous le format kv
                        //KV fragment = (KV) inputStream.readObject();
                        FileReaderWriterImpl file = new FileReaderWriterImpl(fname);
                        file.open("w");
                        //file.write(fragment);
                        file.close();
                    } else {
                        System.out.println("Unknown format: " + fmt);
                    }
                    String response = fname + " WRITTEN";
                    outputStream.write(response);
                } else if (request.startsWith("READ")) {
                    //TODO: Map-Reduce
                    String[] tokens = request.split(" ");
                    String fname = tokens[1];

                    FileReaderWriterImpl file = new FileReaderWriterImpl(fname);
                    file.open("r");

                    String response = "Readind file " + fname + " ...";
                    outputStream.write(response);
                    
                    // Lire le fichier fname
                    KV content = file.read();

                    while (content != null) {
                        //outputStream.write(content);
                        //erreur ne sait pas write kv
                        content = file.read();
                    }

                    content = null;
                    //outputStream.write(content);

                    String end = "END OF FILE";
                    outputStream.write(end);

                    file.close();
                } else {
                    System.out.println("Unknown request: " + request);
                }
                request = inputStream.readLine();
            }
		} catch (Exception ex) {
            ex.printStackTrace();
        }
	}

    public static void main (String args[]) {
		try {
			ServerSocket serverSocket = new ServerSocket(5002);
		    while (true) {
			    Socket clientSocket = serverSocket.accept();
                HdfsServer hdfsServer = new HdfsServer(clientSocket);
                hdfsServer.start();
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
}
