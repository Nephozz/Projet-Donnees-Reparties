package hdfs;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import interfaces.FileReaderWriterImpl;
import interfaces.KV;

public class HdfsClient {
	
	private static void usage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <txt|kv> <file>");
		System.out.println("Usage: java HdfsClient delete <file>");
	}
	
	public static void HdfsDelete(String fname) {
        try {
            HashMap<String,Integer> config = readConfigFile("./config/config.txt");

            String[] machines = config.keySet().toArray(new String[0]);
            int[] ports = config.values().stream().mapToInt(Integer::intValue).toArray();
            int rnd = (int) (Math.random() * machines.length);

            //Socket socket = new Socket(machines[rnd], ports[rnd]);
            Socket socket = new Socket(machines[rnd], 5002);
            System.out.println(machines[rnd]);

            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            //BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            String request = new String("DELETE " + fname);
            System.out.println(request);
        
            out.write(request);
            out.flush();

            //String response = in.readLine();
            //System.out.println(response);

            // Fermer les flux
            //in.close();
            out.close();
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	public static void HdfsWrite(int fmt, String fname) {
		try {
            HashMap<String,Integer> config = readConfigFile("./config/config.txt");

            String[] machines = config.keySet().toArray(new String[0]);
            int[] ports = config.values().stream().mapToInt(Integer::intValue).toArray();

       	 	int numFragments = machines.length;
            int fragmentSize;

            if (fmt == FileReaderWriterImpl.FMT_TXT) {
                BufferedReader bufferedReader = new BufferedReader(new FileReader(fname));
                StringBuilder content = new StringBuilder();
                String line;

                while ((line = bufferedReader.readLine()) != null) {
                    content.append(line).append("\n");
                }
                bufferedReader.close();
                fragmentSize = content.length() / numFragments;

                for (int i = 0; i < numFragments; i++) {
                    int startOffset = i*fragmentSize;
                    int endOffset = (i+1)*fragmentSize;
    
                    String fragment = content.substring(startOffset, endOffset);

                    sendFragment(fragment, fname, fmt, machines[i], ports[i]);
                  }
            } else if (fmt == FileReaderWriterImpl.FMT_KV) {
                FileReaderWriterImpl file = new FileReaderWriterImpl(fname);
                file.open("r");

                List<KV> content = new ArrayList<KV>();
                KV kv = file.read();

                while (kv.k != null) {
                    content.add(kv);
                    kv = file.read();
                }

                file.close();
                fragmentSize = content.size() / numFragments;

                for (int i = 0; i < numFragments; i++) {  
                    KV fragment = new KV(content.get(i).k, content.get(i).v);
                    System.out.println(machines[i]);
                    System.out.println(ports[i]);
                    sendFragment(fragment, fname, fmt, machines[i], ports[i]);
                }
            } else {
                System.out.println("Unknown format: " + fmt);
            }      	
		} catch (Exception e) {
            e.printStackTrace();
        }
	}

    private static void sendFragment(Object fragment, String fname, int fmt, String machine, int port) throws IOException, ClassNotFoundException {

        Socket socket = new Socket(machine, port);
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

        //envoi le fragment au serveur
        //ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
        //ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());

        //marqueur pour dire que c'est ecrire
        String markedFragment = "WRITE " + fname + " " + fmt + "\n" + fragment;
        System.out.println(markedFragment);

        out.write(markedFragment);
        //outputStream.writeObject(markedFragment);

        //String response = (String) inputStream.readObject();
        //System.out.println(response);

        // Fermer les flux
        //outputStream.close();
        //inputStream.close();
        out.close();
        socket.close();
    }

	//lire mon fichier avec mes machines
    //TODO
    //modif a faire si je veux tout executer sur machine perso
    //je veux pas avoir le meme hote et des ports différents
	private static HashMap<String, Integer> readConfigFile(String configFilePath) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(configFilePath));
        HashMap<String, Integer> config = new HashMap<String, Integer>();
        String line;

        while ((line = bufferedReader.readLine()) != null) {
            if (line.startsWith("#")) { continue; }

            String host = line.split(" ")[0];
            int port = Integer.parseInt(line.split(" ")[1]);
            config.put(host, port);
        }
        bufferedReader.close();
        
        return config;
    }

	public static void HdfsRead(String fname) {
            
        try {
            HashMap<String,Integer> config = readConfigFile("./config/config.txt");

            String[] machines = config.keySet().toArray(new String[0]);
            int[] ports = config.values().stream().mapToInt(Integer::intValue).toArray();
            int rnd = (int) (Math.random() * machines.length);

            //Socket socket = new Socket(machines[rnd], ports[rnd]);
            Socket socket = new Socket(machines[rnd], 5002);
            System.out.println(machines[rnd]);

            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            String request = new String("READ " + fname);
            System.out.println(request);

            out.println(request);
            out.flush();

            FileReaderWriterImpl writer = new FileReaderWriterImpl("test-read.txt");
            writer.open("w");

            //String response = (String) in.readLine();
            //System.out.println(response);

            String line;

            while ((line = in.readLine()) != null) {
                System.out.println(line);
                KV kv = new KV(line.substring(1), String.valueOf(line.charAt(0)));
                writer.write(kv);
            }

            String end = (String) in.readLine();
            System.out.println(end);

            // Fermer les flux
            in.close();
            writer.close();
            out.close();
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
                if (option.equals("txt")) {
                    fmt = FileReaderWriterImpl.FMT_TXT;
                } else if (option.equals("kv")) {
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
