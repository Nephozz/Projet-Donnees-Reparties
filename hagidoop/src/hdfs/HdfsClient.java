package hdfs;

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import interfaces.FileReaderWriter;
import interfaces.KV;
import interfaces.Request;
import interfaces.RequestType;
import config.Project;

public class HdfsClient {

    private static HashMap<Integer, String> nodeServers = new HashMap<Integer, String>();
	
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
            int i = 1;
            for (Map.Entry<Integer, String> entry : nodeServers.entrySet()) {
                Socket socket = new Socket(entry.getValue(), entry.getKey());
                ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
               
                String fragmentName = createFragmentName(fname, i);    // Recréer le nom du fichier avec le numéro de fragment
				Request request = new Request(RequestType.DELETE, fragmentName);

                outputStream.writeObject(request);

                try {
                    System.out.println(inputStream.readObject());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                
                inputStream.close();
                outputStream.close();
                socket.close();
                i++;
            } 
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	
	public static void HdfsWrite(int fmt, String fname) {
		try {
       	 	int numFragments = nodeServers.size();
            Path path = Paths.get(fname);
            int fileSize = (int) Files.lines(path).count();

            int fragSize = fileSize / numFragments;

            if (fmt == FileReaderWriter.FMT_TXT) {
                int i = 1;
                for (Map.Entry<Integer, String> entry : nodeServers.entrySet()) {
                    String fragmentName = createFragmentName(fname, i);

                    Socket socket = new Socket(entry.getValue(), entry.getKey());
                    ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                    ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String content = null;
                    String line;

                    int startOffset = i*fragSize;
                    int endOffset = (i+1)*fragSize;

                    for (int j = startOffset; j < endOffset; j++) {
                        line = reader.readLine();
                        if (line != null) {
                            content += line + "\n";
                        }
                    }

					Request request = new Request(RequestType.WRITE, fragmentName);
					request.setFmt(FileReaderWriter.FMT_TXT);
					request.passContent(content);

                    outputStream.writeObject(request);
                    System.out.println(inputStream.readObject());

                    inputStream.close();
                    outputStream.close();
                    socket.close();
                    i++;
                }
            } else if (fmt == FileReaderWriter.FMT_KV) {
                FileReaderWriter reader = new FileReaderWriter(fname);
                int nbLine = (int) reader.size();
                int nbKV = nbLine / nodeServers.size();

                reader.open("r");
                KV[] content;

                int i = 1;
                for (Map.Entry<Integer, String> entry : nodeServers.entrySet()) {
                    String fragmentName = createFragmentName(fname, i);
                    
                    Socket socket = new Socket(entry.getValue(), entry.getKey());
                    ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                    ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
                    content = new KV[nbKV];

                    for (int j = 0; j < nbKV; j++) {
                        content[j] = reader.read();
                    }

                    Request request = new Request(RequestType.WRITE, fragmentName);
					request.setFmt(FileReaderWriter.FMT_KV);
					request.passContent(content);
					
                    outputStream.writeObject(request);
                    System.out.println(inputStream);

                    inputStream.close();
                    outputStream.close();
                    socket.close();
                    i++;
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
            FileReaderWriter writer = new FileReaderWriter(fname);
            writer.open("w");

            int i = 0;
            for (Map.Entry<Integer, String> entry : nodeServers.entrySet()) {
                String fragmentName = createFragmentName(fname, i);

                Socket socket = new Socket(entry.getValue(), entry.getKey());
                ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());

				Request request = new Request(RequestType.READ, fragmentName);
                outputStream.writeObject(request);

                System.out.println(inputStream.readObject());

                KV content = (KV) inputStream.readObject();

                while (content != null) {
                    writer.write(content);
                    content = (KV) inputStream.readObject();
                }

                System.out.println(inputStream.readObject());

                outputStream.close();
                inputStream.close();
                socket.close();
            }     	
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

	// à quoi sert cette méthode ?
    public static String createFragmentName(String fname, int i) {
        int dotIndex = fname.lastIndexOf(".");
        String name = fname.substring(0, dotIndex);
        String format = fname.substring(dotIndex);
        return  name + "-" + i + format;
    }

	public static void main(String[] args) {
		// java HdfsClient <read|write> <txt|kv> <file>
		// appel des méthodes précédentes depuis la ligne de commande
		//en fonction de la ligne de commande et des ses arguments je lance une des méthodes HDFS
		if (args.length < 2) {
            usage();
            return;
        }

        nodeServers = Project.servers;
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
