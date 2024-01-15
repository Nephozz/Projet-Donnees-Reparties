package hdfs;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import interfaces.FileImpl;
import interfaces.FileReaderWriter;
import interfaces.KV;
import hdfs.Request;

public class HdfsClient {

    private static List<String> machines = new ArrayList<>();
    private static List<Integer> ports = new ArrayList<>();
	
	private static void usage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <txt|kv> <file>");
		System.out.println("Usage: java HdfsClient delete <file>");
	}
	
	public static void HdfsDelete(String fname) {
        try {
            for (int i = 0; i < ports.size(); i++) {
                Socket socket = new Socket(machines.get(i), ports.get(i));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                String majFname = majFname(fname, i);
                String request = "DELETE " + majFname;
				//Request request = new Request(RequestType.DELETE, majFname);
                out.println(request);
                out.close();
                socket.close();
            } 
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	public static void HdfsWrite(int fmt, String fname) {
		try {
       	 	int numFragments = machines.size();
            File file = new File(fname);
            int fSize = (int) file.length();

            int fragSize = fSize/numFragments;

            if (fmt == FileReaderWriter.FMT_TXT) {
                FileImpl rw = new FileImpl(fname);
                rw.open("r");

                KV kv;

                for (int i = 0; i < numFragments; i++) {
                    String majFname = majFname(fname, i);

                    Socket socket = new Socket(machines.get(i), ports.get(i));
                    PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                    String content = "";

                    int startOffset = i*fragSize;
                    int endOffset = (i+1)*fragSize;

                    for (int j = startOffset; j < endOffset; j++) {
                        kv = rw.read();
                        if (kv != null) {
                            content += kv.toString() + "\n";
                        }
                    }
                    String request = "WRITE " + majFname + "\n" + content;
					/*
					Request request = new Request(RequestType.WRITE, majFname);
					request.setFmt(FileReaderWriter.FMT_TXT);
					request.passContent(content);
					*/
                    writer.println(request);
                    writer.close();
                    socket.close();
                }
            } else if (fmt == FileReaderWriter.FMT_KV) {
                FileImpl rw = new FileImpl(fname);
                rw.open("r");
                KV kv;

				// que représente fs ?
                for (int i = 0; i < fs; i++) {
                    String majFname = majFname(fname, i);

                    Socket socket = new Socket(machines.get(i), ports.get(i));
                    PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                    String content = "";

                    int startOffset = i*fragSize;
                    int endOffset = (i+1)*fragSize;

                    for (int j = startOffset; j < endOffset; j++) {
                        kv = rw.read();
                        if (kv != null) {
                            content += kv.toString() + "\n";
                        }
                    }
                    String request = "WRITE " + majFname + "\n" + content;
					/*
					Request request = new Request(RequestType.WRITE, majFname);
					request.setFmt(FileReaderWriter.FMT_KV);
					request.passContent(content);
					*/
                    writer.println(request);
                    writer.close();
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
            FileImpl rw = new FileImpl(fname);
            rw.open("w");

            KV kv = new KV();

            for (int i = 0; i < ports.size(); i++) {
                String majFname = majFname(fname, i);

                Socket socket = new Socket(machines.get(i), ports.get(i));
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                String request = "READ " + majFname;
				//Request request = new Request(RequestType.READ, majFname);
                writer.println(request);
                writer.flush();

                String line;

                while ((line = reader.readLine()) != null) {
                    kv.k = line.split(KV.SEPARATOR)[0];
                    kv.v = line.split(KV.SEPARATOR)[1];
                    rw.write(kv);
                }
                writer.close();
                reader.close();
                socket.close();
            }     	
            rw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

	// à quoi sert cette méthode ?
    public static String majFname(String fname, int i) {
        int dot = fname.lastIndexOf(".");
        String name = fname.substring(0, dotIndex);
        String format = fname.substring(dotIndex);
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
