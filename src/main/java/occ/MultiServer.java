package occ;

import java.net.*;
import java.io.*;

public class MultiServer {
    private enum SERVER_STATUS {RUNNING, SHUTDOWN}
    private ServerSocket serverSocket;
    private final int siteId;
    private final TransactionManager serverTM;
    private final int port;
    private int currentConnections;
    private SERVER_STATUS serverStatus;

    public MultiServer(int siteId, TransactionManager serverTM, int port) {
        this.siteId = siteId;
        this.serverTM = serverTM;
        this.port = port;
        this.currentConnections = 0;
        this.serverStatus = SERVER_STATUS.RUNNING;
    }
    // my question: should server be on a new thread? because if we are in the main thread, we will never be able to go to the next line
    // until we connect clients, and to connect clients, we need to go to the next line, therefore deadlocked?!
    public void start() throws IOException {
        serverSocket = new ServerSocket(this.port);

        System.out.println("Site " + this.siteId + " listening for new connections");
        // we should probably make this 4 connections (we are assuming no site failure) --> done
        while (currentConnections < 3) {
            new ClientHandler(serverSocket.accept()).start();
            currentConnections++;
        }
    }

    public void stop() throws IOException {
        serverSocket.close();
    }

    private static class ClientHandler extends Thread {
        private Socket clientSocket;
        private ObjectInputStream inputStream;
        private ObjectOutputStream outputStream;

        public ClientHandler(Socket socket) {
            this.clientSocket = socket;
        }

        public void run(){
            try {
                outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
                inputStream = new ObjectInputStream(clientSocket.getInputStream());
                while (true){
                    Packet request = new Packet((Packet)inputStream.readObject());
                    if(request.getMessage() == Packet.MESSAGES.SHUT_DOWN)break;
                    else{
                        System.out.println("Process the request");
                        // 1 solution here is that we can pass the transaction manager to the server, and perform actions from here
                        // other can be to pass the server to tm, i believe 1 would work
                    }
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
            try {
                outputStream.close();
                inputStream.close();
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
