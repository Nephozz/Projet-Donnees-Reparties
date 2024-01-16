package hdfs;

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.util.List;
import java.util.ArrayList;

import interfaces.FileReaderWriterImpl;
import interfaces.FileReaderWriter;
import interfaces.KV;

public class HdfsClient {

    private static List<String> machines = new ArrayList<>();
    private static List<Integer> ports = new ArrayList<>();
	
	private static void usage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <txt|kv> <file>");
		System.out.println("Usage: java HdfsClient delete <file>");
	}
	/*
     * HdfsDelete : supprime un fichier sur le HDFS
     * il le supprime sur chaque machine
     */
	public static void HdfsDelete(String fname) {
        try {
            for (int i = 0; i < ports.size(); i++) {
                Socket socket = new Socket(machines.get(i), ports.get(i));
                ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
               
                String majFname = majFname(fname, i);    // Recréer le nom du fichier avec le numéro de fragment
				Request request = new Request(RequestType.DELETE, majFname);

                outputStream.writeObject(request);
                outputStream.close();
                socket.close();
            } 
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	/*
     * HdfsWrite : écrit un fichier sur le HDFS
     * il écrit une partie du fichier sur chaque machine
     */
	public static void HdfsWrite(int fmt, String fname) {
		try {
       	 	int numFragments = machines.size();
            File file = new File(fname);
            int fSize = Math.toIntExact(Files.lines(file.toPath()).count());

            int fragSize = fSize/numFragments+1;

            if (fmt == FileReaderWriter.FMT_TXT | fmt == FileReaderWriter.FMT_KV) {
                FileReaderWriterImpl readerWriter = new FileReaderWriterImpl(fname, fmt);
                readerWriter.open("r");
                KV kv;

                for (int i = 0; i < numFragments; i++) {
                    String majFname = majFname(fname, i);

                    Socket socket = new Socket(machines.get(i), ports.get(i));
                    ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                    List<KV> content = new ArrayList<>();

                    int startOffset = i*fragSize;
                    int endOffset = (i+1)*fragSize;

                    for (int j = startOffset; j < endOffset; j++) {
                        kv = readerWriter.read();
                        if (kv != null) {
                            content.add(kv);
                        }
                    }

                    Request request = new Request(RequestType.WRITE, majFname);
					request.passContent(content);

                    outputStream.writeObject(request);
                    outputStream.close();
                    socket.close();
                }
            } else {
                System.out.println("Unknown format: " + fmt);
            }    	
		} catch (Exception e) {
            e.printStackTrace();
        }
	}

	/*
	 * HdfsRead : lit un fichier sur le HDFS et le télécharge localement
	 * il le recupère sous de KV
	 */
	public static void HdfsRead(String fname) {
            
        try {
            int fmt = FileReaderWriter.FMT_KV;
            FileReaderWriterImpl readerWriter = new FileReaderWriterImpl(fname, fmt);
            readerWriter.open("w");

            for (int i = 0; i < ports.size(); i++) {
                String majFname = majFname(fname, i);

                Socket socket = new Socket(machines.get(i), ports.get(i));
                ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                
                //j'envoie la requète
				Request request = new Request(RequestType.READ, majFname);
                outputStream.writeObject(request);
                outputStream.close();
                
                //je récup le fichier que j'ai demandé
                ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
                
                Request receivedRequest = (Request) inputStream.readObject();
                List<KV> receivedContent = (List<KV>) receivedRequest.content;
                
                for (KV kv : receivedContent) {
                    readerWriter.write(kv);
                }

                inputStream.close();
                socket.close();
            }
            readerWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

	/*
	 * majFname : rajoute le numéro du fragment après le nom de fichier
     */
    public static String majFname(String fname, int i) {
        int dot = fname.lastIndexOf(".");
        String name = fname.substring(0, dot);
        String format = fname.substring(dot);
        return  name + "-" + i + format;
    }
    
	/*
	 * readConfigFile : lit le fichier de configuration et remplit les listes machines et ports
	 */
	private static void readConfigFile(String configFilePath) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(configFilePath));
        String line;

        while ((line = reader.readLine()) != null) {
            if (line.startsWith("#")) { continue; }

            String host = line.split(" ")[0];
            int port = Integer.parseInt(line.split(" ")[1]);
            
            machines.add(host);
            ports.add(port);
        }
        reader.close();
    }

	public static void main(String[] args) {
		// java HdfsClient <read|write> <txt|kv> <file>
		// appel des méthodes précédentes depuis la ligne de commande
		//en fonction de la ligne de commande et des ses arguments je lance une des méthodes HDFS
		if (args.length < 2) {
            usage();
            return;
        }
		try {
			readConfigFile("./config/config.txt");
		} catch (Exception e) {
			e.printStackTrace();
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
                    fmt = FileReaderWriter.FMT_TXT;
                } else if (option.equals("kv")) {
                    fmt = FileReaderWriter.FMT_KV;
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
