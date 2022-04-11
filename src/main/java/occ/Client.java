package occ;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.*;

public class Client {
    private Socket clientSocket;
    private ObjectOutputStream outputStream;
    private ObjectInputStream inputStream;

    public void startConnection(String ip, int port) throws IOException {
        clientSocket = new Socket(ip, port);
        outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
        inputStream = new ObjectInputStream(clientSocket.getInputStream());
    }

    public Packet sendMessage(Packet packet) {
//        out.println(msg);
//        String resp = in.readLine();
//        return resp;
        try {
            outputStream.writeObject(packet);
//            outputStream.flush(); // not sure if needed or not
        } catch (IOException e) {
            e.printStackTrace();
        }
        return packet;
    }

    public void stopConnection() throws IOException {
        outputStream.close();
        inputStream.close();
        clientSocket.close();
    }
}
