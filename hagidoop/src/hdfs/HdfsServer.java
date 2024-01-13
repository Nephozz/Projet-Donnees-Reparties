package hdfs;

//rajouter des méthodes read, write, delete

public class HdfsServer {
	
    public static void main (String args []) {
		try {
			ServerSocket ss = new ServerSocket(8080);
		    
            DistribThreadServer dts = new DistribThreadServer(8080, ss);
            

            ss.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
}

public class DistribThreadServer implements Thread {
    private ServerSocket ss;

    private int port;

    public DistribThreadServer(int port, ServerSocket ss) {
        this.port = port;
        this.ss = ss;
    }

    public void run() {
        while (!serverSocket.isClosed()) {
            Socket cs = ss.accept();
        }
    }
}

public class ThreadServer implements Thread {

    private Socket s;

    private TreadServer(Socket s) {
        this.s = s;
    }

    public void run() {
        try {
            InputStream in = s.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            String message = reader.readLine();

            char action = message.charAt(0);
            String fragment = message.substring(1);

            switch (action) {
                case '0':
                    handleRead(fragment);
                    break;
                case '1':
                    handleWrite(fragment);
                    break;
                case '2':
                    handleDelete(fragment);
                    break;
                default:
                    break;
            }

        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            try {
                s.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleRead(String fragment) {
    }

    private void handleWrite(String fragment) {
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFileName));
        writer.write(fragment.substring(endIndex + 1));
        writer.close();
    }
    private void handleWrite(String fragment) {
        try {
            // Extraire le nom du fichier d'origine du fragment
            int endIndex = fragment.indexOf(' ');
            String originalFileName = fragment.substring(0, endIndex);

            // Créer le nom du fichier de sortie avec le suffixe numérique
            int fragmentNumber = Integer.parseInt(fragment.substring(endIndex + 1));
            String outputFileName = originalFileName + "-" + fragmentNumber + ".txt";

            // Écrire le fragment dans le fichier de sortie
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFileName));
            writer.write(fragment.substring(endIndex + 1));
            writer.close();

            System.out.println("Fragment écrit dans : " + outputFileName);
        }
    }
    private void handleDelete(String fragment) {
    }

    private 
}
