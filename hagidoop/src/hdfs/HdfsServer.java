package hdfs;

//rajouter des méthodes read, write, delete

public class HdfsServer {
	
    private Socket s;

    private HdfsServer(Socket s) {
        this.s = s;
    }
	
    public static void main (String args []) {
		try {
			ServerSocket ss = new ServerSocket(8080);
		    while (true) {
			    Socket cs = ss.accept();
                ThreadServer thread = new ThreadServer(cs);
                thread.start();
			}
		} catch (IOException ex) {
			ex.printStackTrace();
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
    }

    private void handleDelete(String fragment) {
    }
}
