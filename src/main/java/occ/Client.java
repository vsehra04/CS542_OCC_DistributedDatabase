package occ;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.*;

public class Client implements Runnable{
    private Socket clientSocket;
    private ObjectOutputStream outputStream;
    private ObjectInputStream inputStream;
    private boolean run;
    private final int connectingSite;
    private final int initiatingSite;
    private final TransactionManager clientTM;
    private final String ip;
    private final int port;

    public Client(String ip, int port, int siteId, int initiatingSite,TransactionManager tm){
        this.clientTM = tm;
        this.connectingSite = siteId;
        this.ip = ip;
        this.port = port;
        this.initiatingSite = initiatingSite;
    }

    public void startConnection() {
        run = true;
        try {
            clientSocket = new Socket();
            clientSocket.connect(new InetSocketAddress(ip, port), 10000);
            outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
            inputStream = new ObjectInputStream(clientSocket.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
//        this.sendMessage(new Packet())
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
        run = false;
    }

    @Override
    public void run() {
        // message listener
        while(run){
            try {
                Packet response = new Packet((Packet)inputStream.readObject());

                if(response.getMessage() == Packet.MESSAGES.SHUT_DOWN)break;
                else if(response.getMessage() == Packet.MESSAGES.ABORT){
                    System.out.println("Abort this transaction in my site");
                }
                else if(response.getMessage() == Packet.MESSAGES.ACK){
                    System.out.println("This will only happen at the initiating site, and here we will add +1 to the semi-commited map");
                }
                else{
                    // GLOBAL COMMIT
                    System.out.println("convert a semi-committed transaction to committed");
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
