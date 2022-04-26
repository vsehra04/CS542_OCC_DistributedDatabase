package occ;

import javax.swing.plaf.IconUIResource;
import java.net.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class MultiServer{
    private enum SERVER_STATUS {RUNNING, SHUTDOWN}
    private ServerSocket serverSocket;
    private final int siteId;
    private final TransactionManager serverTM;
    private final int port;
    private int currentConnections;
    private SERVER_STATUS serverStatus;
    private List<Socket> clientSocket;
    private List<ObjectInputStream> inputStream;
    private List<ObjectOutputStream> outputStream;
    private ReentrantLock lock;

    public MultiServer(int siteId, TransactionManager serverTM, int port) {
        this.siteId = siteId;
        this.serverTM = serverTM;
        this.port = port;
        this.currentConnections = 0;
        this.serverStatus = SERVER_STATUS.RUNNING;
        clientSocket = new ArrayList<>();
        inputStream = new ArrayList<>();
        outputStream = new ArrayList<>();
        lock = new ReentrantLock();
    }

    public void startServer() throws IOException {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    serverSocket = new ServerSocket(port);
                    System.out.println("Site " + siteId + " listening for new connections");
                    while (currentConnections < 3) {
                        startListeningToClient(serverSocket.accept());
                        ++currentConnections;
                    }
                    System.out.println("Site " + siteId + " is ready");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
    }

    private void startListeningToClient(Socket cs) {
        Thread thread = new Thread(new Runnable(){
            @Override
            public void run() {
                ObjectOutputStream os;
                ObjectInputStream is;
                clientSocket.add(cs);
                try {
                    os = new ObjectOutputStream(cs.getOutputStream());
                    is = new ObjectInputStream(cs.getInputStream());
                   outputStream.add(os);
                   inputStream.add(is);

                    while (true){
                        Packet request = new Packet((Packet)is.readObject());
                        if(request.getMessage() == Packet.MESSAGES.SHUT_DOWN)break;
                        else if(request.getMessage() == Packet.MESSAGES.ACK){
                            System.out.println("Ack message received from a site ID " + request.getSiteId() + " for transaction: " + request.getTransaction().getTransactionId() + "at site id " + siteId);
                            lock.lock();
                            try {
                                int count = serverTM.incrementAndGetSemiCommittedTransactions(request.getTransaction());
                                serverTM.getClock().updateTime((int) request.getTime());
                                if (count == 3) {
                                    sendAll(Packet.MESSAGES.GLOBAL_COMMIT, request.getTransaction());
                                }
                            }
                            finally {
                                lock.unlock();
                            }
                        }
                    }

                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
    }

    private void sendPacket(Transaction transaction, Packet.MESSAGES message){
        for(int i=0;i<clientSocket.size();i++){
            try {
                Packet packet = new Packet(serverTM.getClock().getTime(), transaction, message, siteId);
                outputStream.get(i).writeObject(packet);
                outputStream.get(i).flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public void sendAll(Packet.MESSAGES message, Transaction transaction) {
        if(message == Packet.MESSAGES.GLOBAL_COMMIT){
            serverTM.globalCommit(transaction);
            System.out.println("Sending global commit to all sited for transaction : " + transaction.getTransactionId());
            sendPacket(transaction, message);
        }
        else if(message == Packet.MESSAGES.VALIDATE){
            System.out.println("Sending validate to all sited for transaction : " + transaction.getTransactionId());
            sendPacket(transaction, message);
        }
        else if(message == Packet.MESSAGES.ABORT){
            System.out.println("Sending abort to all sited for transaction : " + transaction.getTransactionId());
            sendPacket(transaction, message);
        }
    }

    public void stop() throws IOException {
        serverSocket.close();
        for(int i=0;i<inputStream.size();i++){
            inputStream.get(i).close();
            outputStream.get(i).close();
            clientSocket.get(i).close();
        }
    }

}
