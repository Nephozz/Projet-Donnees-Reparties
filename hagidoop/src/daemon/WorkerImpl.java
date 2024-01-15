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
        m.map(reader, writer);
    }

    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);

        try {
            Registry registry = LocateRegistry.createRegistry(port);
        } catch (Exception e) {
            System.out.println("Port 8080 is already in use.");
        }
        try {
            WorkerImpl worker = new WorkerImpl();
            Naming.rebind("rmi://localhost:8080/worker", worker);
            System.out.println("Worker is ready.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}