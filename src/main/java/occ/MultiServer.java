package occ;

import java.net.*;
import java.io.*;

public class MultiServer {
    private ServerSocket serverSocket;

    public void start(int port) throws IOException {
        serverSocket = new ServerSocket(port);
        // we should probably make this 4 connections (we are assuming no site failure)
        while (true) {
            new ClientHandler(serverSocket.accept()).start();
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
                    Packet request = new Packet();
                    request = (Packet)inputStream.readObject();

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
