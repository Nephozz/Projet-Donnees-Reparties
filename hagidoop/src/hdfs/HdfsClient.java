package hdfs;

public class HdfsClient {

	public static final int FMT_TXT = 0;
    public static final int FMT_KV = 1;
	
	private static void usage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <txt|kv> <file>");
		System.out.println("Usage: java HdfsClient delete <file>");
	}
	
	public static void HdfsDelete(String fname) {
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

        	String[] machines, ports = readConfigFile("\\wsl.localhost\Ubuntu\home\margaux\hagidoop\config\config.txt");

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
        		String markedFragment = "1"+fname+"-"+i+" "+fragment;

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
	private static String[] readConfigFile(String configFilePath) throws IOException {
        return;
    }

	public static void HdfsRead(String fname) {
	}

	public static void main(String[] args) {
		// java HdfsClient <read|write> <txt|kv> <file>
		// appel des méthodes précédentes depuis la ligne de commande
		//en fonction de la ligne de commande et des ses arguments je lance une des méthodes HDFS
		if (args.length < 2) {
            usage();
            return;
        }
		///test
		
		String action = args[0];

		switch (action) {
			case "read":
				HdfsRead(arg[1]);
				break;
			case "write":
				String format = args[1];
				int fileFormat;
				if (format.equals("txt")) {
                    fileFormat = FMT_TXT;
                } else if (format.equals("kv")) {
                    fileFormat = FMT_KV;
                } else {
                    usage();
                    return;
                } 
				HdfsWrite(fileFormat, args[2])
				break;
			case "delete":
				HdfsDelete(args[1]);
				break;
			default:
				usage();
				return;
		}
	}
	
} // 300 mo 3 sleve le count 3 sec et avec hagidoop 1300
