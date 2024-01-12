package hdfs;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

//rajouter des méthodes read, write, delete

public class HdfsServer {
	
    private Socket s;
    private int num;

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
    private int num;

    private TreadServer(Socket s) {
        this.s = s;
    }

    public void run () {
		try {
			InputStream in = cs.getInputStream();
			OutputStream out = cs.getOutputStream();

            ObjectInputStream oin = new ObjectInputStream(in);
            ObjectOutputStream oout = new ObjectOutputStream(out);
                
            }
		} catch (IOException | ClassNotFoundException ex) {
            ex.printStackTrace();
        } finally {
            try {
                s.close();
                in.close();
                out.close();
                oin.close();
                oout.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
	}
