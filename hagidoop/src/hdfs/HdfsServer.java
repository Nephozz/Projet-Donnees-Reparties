package hdfs;

import java.io.*;
import java.net.*;

import interfaces.KV;
import interfaces.FileReaderWriter;

import config.Project;

//modif pour m'adapter au nouveau client
//manque un séparateur sur différent thread
public class HdfsServer implements Runnable {
    private Socket client;

    public HdfsServer(Socket s) {
        this.client = s;
    }

    public void run () {
		try {
            Socket clientSocket = this.client;

            ObjectOutputStream outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
            ObjectInputStream inputStream = new ObjectInputStream(clientSocket.getInputStream());
            //BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            
            Request request = (Request) inputStream.readObject();

            while (request != null) {
                String fname = request.fname;
                String response;

                switch (request.type) {
                    case RequestType.DELETE:
                        new File(fname).delete();

                        response = fname + " DELETED";
                        outputStream.writeObject(response);   
                        break;
                    case RequestType.WRITE:
                        int fmt = request.fmt;

                        if (fmt == FileReaderWriter.FMT_TXT) {
                            writeFromTXTFile(request, fname);
                        } else if (fmt == FileReaderWriter.FMT_KV) {
                            writeFromKVFile(request, fname);
                        } else {
                            System.out.println("Unknown format: " + fmt);
                        }

                        response = fname + " WRITTEN";
                        outputStream.writeObject(response);
                        break;
                    case RequestType.READ:
                        FileReaderWriter file = new FileReaderWriter(fname);
                        file.open("r");

                        response = "Readind file " + fname + " ...";
                        outputStream.writeObject(response);
                        
                        // Lire le fichier fname
                        KV content = file.read();

                        while (content != null) {
                            outputStream.writeObject(content);;
                            //erreur ne sait pas write kv
                            content = file.read();
                        }

                        content = null;
                        outputStream.writeObject(content);

                        String end = "\nEND OF FILE";
                        outputStream.writeObject(end);

                        file.close();
                        break;
                    default:
                        System.out.println("Unknown request: " + request);
                        break;
                }
                request = (Request) inputStream.readObject();
            }
		} catch (Exception ex) {
            ex.printStackTrace();
        }
	}

    private void writeFromTXTFile(Request request, String fname) {
        FileReaderWriter file = new FileReaderWriter(fname);
        file.open("w");

        String content = (String) request.content;
        String[] lines = content.split("\\r?\\n");

        for (String line : lines) {
            KV kv = new KV(line, String.valueOf(file.getIndex()));
            file.write(kv);
        }
        file.close();
    }

    private void writeFromKVFile(Request request, String fname) {
        // Ecrire le fragment sous le format kv
        FileReaderWriter file = new FileReaderWriter(fname);
        file.open("w");

        KV[] content = (KV[]) request.content;
        for (KV kv : content) {
            file.write(kv);
        }
        
        file.close();
    }

    public static void main (String args[]) {
		try {
            //marche que sur le port 5002 a changer !!!!
			ServerSocket serverSocket = new ServerSocket(Project.HDFS_PORT);

		    while (true) {
			    new Thread(new HdfsServer(serverSocket.accept()));
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
}