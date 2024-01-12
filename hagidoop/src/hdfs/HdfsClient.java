package hdfs;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class HdfsClient {
    public enum FMT {
        TXT,
        KV
    }
	
	private static void usage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <txt|kv> <file>");
		System.out.println("Usage: java HdfsClient delete <file>");
	}
	
	public static void HdfsDelete(String fname) {
        Socket socket = new Socket(serverAddress, serverPort);
        ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
        ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
	}
	
	public static void HdfsWrite(FMT fmt, String fname) {
        Socket socket = new Socket(serverAddress, serverPort);
        ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
        ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
        
        switch (fmt) {
            case TXT:
                // write txt
                break;
            case KV:
                // write kv
                break;
            default:
                usage();
                System.exit(1);
        }
	}

	public static void HdfsRead(String fname) {
        Socket socket = new Socket(serverAddress, serverPort);
        ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
        ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream())

	}

	public static void main(String[] args) {
		// java HdfsClient <read|write> <txt|kv> <file>
		// appel des méthodes précédentes depuis la ligne de commande
		if (args.length < 2) {
            usage();
            System.exit(1);
        }

        String operation = args[0];
        String fileName = args[args.length - 1];

		switch (operation) {
            case "read":
                HdfsRead(fileName);
                break;
            case "write":
				// à modif si kv ou text traiter les options
                String option = args[1];
                if (option == "txt") {
                    HdfsWrite(FMT.TXT, fileName);
                } else if (option == "kv") {
                    HdfsWrite(FMT.KV, fileName);
                } else {
                    usage();
                    System.exit(1);
                }
                break;
            case "delete":
                HdfsDelete(fileName);
                break;
            default:
                usage();
                System.exit(1);
        }
	}
}
