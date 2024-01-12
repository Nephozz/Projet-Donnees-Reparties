package hdfs;

import java.io.*;
import java.net.*;
import java.util.HashMap;

import interfaces.KV;
import hdfs.FileReaderWriterImpl;

public class HdfsClient {

	public static final int FMT_TXT = 0;
    public static final int FMT_KV = 1;
	
	private static void usage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <txt|kv> <file>");
		System.out.println("Usage: java HdfsClient delete <file>");
	}
	
	public static void HdfsDelete(String fname) {
        Socket socket = new Socket(serverAddress, serverPort);
        OutputStream outputStream = socket.getOutputStream();
        InputStream inputStream = socket.getInputStream();

        byte[] buffer = new byte[1024];
        int nbLu;

        try {
            buffer = fname.getBytes();
            outputStream.write(buffer, 0, nbLu);
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	public static void HdfsWrite(int fmt, String fname) {
		try {
        	BufferedReader br = new BufferedReader(new FileReader(fname));
        	StringBuilder content = new StringBuilder();
        	String line;

        	while ((line = br.readLine()) != null) {
            	content.append(line).append("\n");
        	}
        	br.close();

        	HashMap<String,Integer> config = readConfigFile("./hagidoop/config/config.txt");

            String[] machines = config.keySet().toArray(new String[0]);
            int[] ports = config.values().stream().mapToInt(Integer::intValue).toArray();

       	 	int numFragments = machines.length;
        	int fragmentSize = content.length() / numFragments;

        	for (int i = 0; i < numFragments; i++) {
          		int startOffset = i*fragmentSize;
            	int endOffset = (i+1)*fragmentSize;

            	String fragment = content.substring(startOffset, endOffset);

				Socket socket = new Socket(machines[i], ports[i]);

        		//envoi le fragment au serveur
        		OutputStream out = socket.getOutputStream();
        		ObjectOutputStream oout = new ObjectOutputStream(out);

        		//marqueur pour dire que c'est ecrire
        		String markedFragment = "1 "+fragment;

        		oout.writeObject(markedFragment);
        		oout.flush();

        		// Fermer les flux
        		oout.close();
        		out.close();
        		socket.close();
        	}
		} catch (IOException e) {
            e.printStackTrace();
        }
	}

	//lire mon fichier avec mes machines
	private static HashMap<String, Integer> readConfigFile(String configFilePath) throws IOException {
        return;
    }

	public static void HdfsRead(String fname) {
        Socket socket = new Socket(serverAddress, serverPort);
        OutputStream outputStream = socket.getOutputStream();
        InputStream inputStream = socket.getInputStream();

        byte[] buffer = new byte[1024];
        int nbLu;

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fname))) {
            buffer = fname.getBytes();
            nbLu = inputStream.read(buffer);
            outputStream.write(buffer, 0, nbLu);

            buffer = inputStream.readAllBytes();
            String str = new String(buffer);
            nbLu = inputStream.read(buffer);
            writer.write(str);

            System.out.println("File written successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }
	}

	public static void main(String[] args) {
		// java HdfsClient <read|write> <txt|kv> <file>
		// appel des méthodes précédentes depuis la ligne de commande
		//en fonction de la ligne de commande et des ses arguments je lance une des méthodes HDFS
		if (args.length < 2) {
            usage();
            return;
        }
		
		String action = args[0];

		switch (action) {
            case "read":
                HdfsRead(args[1]);
                break;
            case "write":
				// à modif si kv ou text traiter les options
                String option = args[1];
                int fmt = -1;
                if (option == "txt") {
                    fmt = FileReaderWriterImpl.FMT_TXT;
                } else if (option == "kv") {
                    fmt = FileReaderWriterImpl.FMT_KV;
                } else {
                    usage();
                    System.exit(1);
                }
                HdfsWrite(fmt, args[2]);
                break;
            case "delete":
                HdfsDelete(args[1]);
                break;
            default:
                usage();
                System.exit(1);
        }
	}
}
// 300 mo 3 sleve le count 3 sec et avec hagidoop 1300
