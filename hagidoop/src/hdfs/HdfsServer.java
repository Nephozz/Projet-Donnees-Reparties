package hdfs;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class HdfsServer implements Runnable {
	
    private Socket s;
    private int num;

    private HdfsServer(Socket s) {
        this.s = s;
    }
	
    public static void main (String args []) {
		try {
			ServerSocket ss = new ServerSocket(8080);
		while (true) {
			Socket s = ss.accept();
			new Thread(new HdfsServer(s)).start();
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

    public void run () {
		try {
			InputStream in = s.getInputStream();
			OutputStream out = s.getOutputStream();

            ObjectInputStream oin = new ObjectInputStream(in);
            ObjectOutputStream oout = new ObjectOutputStream(out);

            Integer[] tab = (Integer[]) oin.readObject();

            this.num = tab[0];
            int request = tab[1];

            if (request == 0) {
                int bufferSize = tab[3];
                byte[] buffer = new byte[bufferSize];
                Integer bytesRead = in.read(buffer);
                if (bytesRead>0) {
                    this.whriteFile(buffer);
                }
            }
            if (request == 1) {
                
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
}
