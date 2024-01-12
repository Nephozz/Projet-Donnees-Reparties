package hdfs;

public class HdfsClient {
	
	private static void usage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <txt|kv> <file>");
		System.out.println("Usage: java HdfsClient delete <file>");
	}
	
	public static void HdfsDelete(String fname) {
	}
	
	public static void HdfsWrite(int fmt, String fname) {
	}

	public static void HdfsRead(String fname) {
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
                    HdfsWrite(0, fileName);
                } else if (option == "kv") {
                    HdfsWrite(1, fileName);
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
