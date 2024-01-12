import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class SimpleServer {

    public static void main(String[] args) {
        try {
            ServerSocket serverSocket = new ServerSocket(5000);
            System.out.println("Serveur en attente de connexion...");

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connecté.");

                BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

                while (true) {
                    String message = reader.readLine();
                    if (message == null || message.equals("exit")) {
                        break;
                    }
                    System.out.println("Client: " + message);
                }
                clientSocket.close();
                System.out.println("Client déconnecté.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
