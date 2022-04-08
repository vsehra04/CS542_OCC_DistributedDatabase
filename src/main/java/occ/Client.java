package occ;

import java.io.IOException;
import java.net.*;

public class Client {
    private Socket clientSocket;

    public void startConnection(String ip, int port) throws IOException {
        clientSocket = new Socket(ip, port);
//        out = new PrintWriter(clientSocket.getOutputStream(), true);
//        in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
    }

    public Packet sendMessage(Packet packet) {
//        out.println(msg);
//        String resp = in.readLine();
//        return resp;
        return packet;
    }

    public void stopConnection() throws IOException {
//        in.close();
//        out.close();
        clientSocket.close();
    }
}
