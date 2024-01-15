package hdfs;

import java.io.*;
import java.net.*;

public class HdfsServer {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java HdfsServer machine port");
            System.exit(1);
        }
        String machine = args[0];
        int port = Integer.parseInt(args[1]);
        try {
            ServerSocket serverSocket = new ServerSocket(port);

            while (!serverSocket.isClosed()) {
                try {
                    Socket client = serverSocket.accept();
                    HdfsServerThread serverThread = new HdfsServerThread(client);
                    serverThread.start();
                } catch (SocketException e) {
                    e.printStackTrace();
                }
            }
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}