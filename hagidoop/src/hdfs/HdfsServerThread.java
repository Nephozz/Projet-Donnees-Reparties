package hdfs;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;

import interfaces.FileReaderWriter;
import interfaces.FileReaderWriterImpl;
import interfaces.KV;

public class HdfsServerThread extends Thread {
    private Socket client;

    public HdfsServerThread(Socket s) {
        this.client = s;
    }

    public void run () {
		try {
            ObjectInputStream inputStream = new ObjectInputStream(client.getInputStream());
                
            Request receivedRequest = (Request) inputStream.readObject();
            RequestType typeRequest = receivedRequest.type;
            int fmt = FileReaderWriter.FMT_KV;

            while (receivedRequest != null) {
                if (typeRequest == RequestType.DELETE) {
                    String fname = receivedRequest.fname;
                    // Supprimer le fichier fname
                    File file = new File(fname);
                    file.delete();
                } else if (typeRequest == RequestType.WRITE) {
                    String fname = receivedRequest.fname;
                    List<KV> receivedContent = (List<KV>) receivedRequest.content;

                    FileReaderWriterImpl readerWriter = new FileReaderWriterImpl(fname, fmt);
                    readerWriter.open("w");

                    for (KV kv : receivedContent) {
                        readerWriter.write(kv);
                    }
                    readerWriter.close();
                } else if (typeRequest == RequestType.READ) {
                    String fname = receivedRequest.fname;

                    FileReaderWriterImpl readerWriter = new FileReaderWriterImpl(fname, fmt);
                    readerWriter.open("r");

                    ObjectOutputStream outputStream = new ObjectOutputStream(client.getOutputStream());
                    List<KV> content = new ArrayList<>();
                    KV kv;

                    while ((kv = readerWriter.read()) != null) {
                        content.add(kv);
                    }

                    Request request = new Request(RequestType.READ, fname);
					request.passContent(content);
                    outputStream.writeObject(request);

                    readerWriter.close();
                    outputStream.close();
                } else {
                    System.out.println("Unknown request");
                }
            }
		} catch (Exception ex) {
            ex.printStackTrace();
        }
	}
}
