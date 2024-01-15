package daemon;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.*;
import java.rmi.Naming;

import interfaces.FileReaderWriter;
import interfaces.NetworkReaderWriter;
import interfaces.Map;

public class WorkerImpl extends UnicastRemoteObject implements Worker {
    public WorkerImpl() throws RemoteException {}

    public void runMap (Map m, FileReaderWriter reader, NetworkReaderWriter writer) throws RemoteException {
        try {
            writer.openServer();
            reader.open("r");
            m.map(reader, writer);
            reader.close();
            writer.closeServer();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void usage() {
        System.out.println("Usage: java WorkerImpl <host> <port>");
    }

    public static void main(String[] args) {
        String host = args[0];
        int port = Integer.parseInt(args[1]);

        if (args.length != 2) {
            try {
                WorkerImpl worker = new WorkerImpl();
                worker.usage();
                System.exit(1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        try {
            Registry registry = LocateRegistry.createRegistry(port);
        } catch (Exception e) {
            System.out.println("Port is already in use.");
        }
        try {
            WorkerImpl worker = new WorkerImpl();
            Naming.rebind("rmi://" + host + ":" + port + "/Worker", worker);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}