package hdfs;

import java.io.*;
import java.net.*;

import interfaces.FileImpl;
import interfaces.KV;

//modif pour m'adapter au nouveau client
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
            System.out.println(request);

            while (request != null) {

                if (request.startsWith("DELETE")) {
                    String[] tokens = request.split(" ");
                    String fname = tokens[1];
                    // Supprimer le fichier fname
                    File file = new File(fname);
                    file.delete();
                    String response = fname + " DELETED";
                    outputStream.write(response);
                } else if (request.startsWith("WRITE")) {
                    String[] tokens = request.split(" ");
                    String fname = tokens[1];
                    int fmt = Integer.parseInt(tokens[2]);

                    if (fmt == 0) {
                        // Ecrire le fragment sous le format txt
                        String fragment = "";
                        String extract;
                        while ((extract = inputStream.readLine())!=null) {
                            fragment = fragment + "\n" + extract;
                        }
                        //a changer quand yaura plusieurs server mettre nomfichier-numeroduserveur
                        FileImpl file = new FileImpl("test-server.txt");
                        file.open("w");
                        String[] lines = fragment.split("\n");

                        for (String line : lines) {
                            System.out.println(line);
                            KV kv = new KV(line, String.valueOf(file.getIndex()));
                            file.write(kv);
                        }
                        file.close();
                    } else if (fmt == 1) {
                        // Ecrire le fragment sous le format kv
                        //KV fragment = (KV) inputStream.readObject();
                        FileImpl file = new FileImpl(fname);
                        file.open("w");
                        //file.write(fragment);
                        file.close();
                    } else {
                        System.out.println("Unknown format: " + fmt);
                    }
                    String response = fname + " WRITTEN";
                    outputStream.write(response);
                } else if (request.startsWith("READ")) {
                    System.out.println(1);
                    //TODO: Map-Reduce
                    String[] tokens = request.split(" ");
                    String fname = tokens[1];

                    FileImpl file = new FileImpl(fname);
                    file.open("r");

                    //String response = "Readind file " + fname + " ...";
                    //outputStream.write(response);
                    
                    // Lire le fichier fname
                    KV content = file.read();
                    System.out.println(content.k);
                    content = file.read();

                    while (content.k != null) {
                        System.out.println(content.k);
                        outputStream.println(content.toString());
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
            //marche que sur le port 5002 a changer !!!!
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
