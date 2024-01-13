package hdfs;

import java.io.*;
import java.net.*;
import java.util.HashMap;

import interfaces.KV;
import hdfs.FileReaderWriterImpl;

public class HdfsClient {
	
	private static void usage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <txt|kv> <file>");
		System.out.println("Usage: java HdfsClient delete <file>");
	}
	
	public static void HdfsDelete(String fname) {
        try {
            HashMap<String,Integer> config = readConfigFile("./hagidoop/config/config.txt");

            String[] machines = config.keySet().toArray(new String[0]);
            int[] ports = config.values().stream().mapToInt(Integer::intValue).toArray();
            int rnd = (int) (Math.random() * machines.length);

            Socket socket = new Socket(machines[rnd], ports[rnd]);

            ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());

            String request = new String("DELETE " + fname);
        
            outputStream.writeObject(request);

            String response = (String) inputStream.readObject();
            System.out.println(response);

            // Fermer les flux
            inputStream.close();
            outputStream.close();
            socket.close();
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
                ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());

        		//envoi le fragment au serveur
        		ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());

        		//marqueur pour dire que c'est ecrire
        		String markedFragment = new String("WRITE "+ fmt + " " + fragment);

        		outputStream.writeObject(markedFragment);
        		outputStream.flush();

                String response = (String) inputStream.readObject();
                System.out.println(response);

        		// Fermer les flux
        		outputStream.close();
                inputStream.close();
        		socket.close();
        	}
		} catch (Exception e) {
            e.printStackTrace();
        }
	}

	//lire mon fichier avec mes machines
	private static HashMap<String, Integer> readConfigFile(String configFilePath) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(configFilePath));
        HashMap<String, Integer> config = new HashMap<String, Integer>();
        String line;

        while ((line = br.readLine()) != null) {
            config.put(line.split(" ")[0], Integer.parseInt(line.split(" ")[1]));
        }
        br.close();
        
        return config;
    }

	public static void HdfsRead(String fname) {
            
        try {
            HashMap<String,Integer> config = readConfigFile("./hagidoop/config/config.txt");

            String[] machines = config.keySet().toArray(new String[0]);
            int[] ports = config.values().stream().mapToInt(Integer::intValue).toArray();
            int rnd = (int) (Math.random() * machines.length);

            Socket socket = new Socket(machines[rnd], ports[rnd]);
            ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());

            String request = new String("READ " + fname);

            BufferedWriter writer = new BufferedWriter(new FileWriter(fname));
            outputStream.writeObject(request);

            String str = inputStream.readUTF();
            writer.write(str);

            System.out.println("File written successfully. \n");
            System.out.println("File contents: " + str);

            // Fermer les flux
            writer.close();
            inputStream.close();
            outputStream.close();
            socket.close();
        } catch (Exception e) {
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
