package interfaces;

import java.io.*;
import java.net.*;

import config.Project;

public class NetworkReaderWriter extends Thread implements ReaderWriter {
	private ServerSocket serverSocket;
	private Socket socket;

	private ObjectInputStream inputStream;
	private ObjectOutputStream outputStream;

	public NetworkReaderWriter(Socket client) {
		this.socket = client;
	}

	public KV read() {
		KV kv = null;
		try {
			inputStream = new ObjectInputStream(socket.getInputStream());
        	kv = (KV) this.inputStream.readObject();
			inputStream.close();
		} catch (Exception e) {
			e.printStackTrace();
		} 
		return kv;
    }

    public void write(KV kv) {
		try {
			outputStream = new ObjectOutputStream(socket.getOutputStream());
			outputStream.writeObject(kv);
			outputStream.flush();
			outputStream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

    public void openServer() throws IOException {
		this.serverSocket = new ServerSocket(Project.NRW_PORT);
    }

	public void opensocket() {
		this.socket = new Socket();
    }

	public NetworkReaderWriter accept() throws IOException {
        Socket client = this.serverSocket.accept();
		return new NetworkReaderWriter(client);
    }

	public void closeServer() throws IOException {
		this.serverSocket.close();
    }

	public void closesocket() throws IOException {
		this.socket.close();
    }
}
