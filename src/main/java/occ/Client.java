package occ;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.net.*;

public class Client implements Runnable{
    private Socket clientSocket;
    private ObjectOutputStream outputStream;
    private ObjectInputStream inputStream;
    private boolean running;
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
        running = true;
        try {
            clientSocket = new Socket();
            clientSocket.connect(new InetSocketAddress(ip, port), 5000);
            outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
            inputStream = new ObjectInputStream(clientSocket.getInputStream());
            Thread thread = new Thread(this, "clientThread");
            thread.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
//        this.sendMessage(new Packet())
    }

    public Packet sendMessage(Packet packet) {
//        out.println(msg);
//        String resp = in.readLine();
//        return resp;
        //if(packet instanceof Packet) System.out.println(packet);
        try {
            outputStream.writeObject(packet);
            outputStream.flush(); // not sure if needed or not
        } catch (IOException e) {
            e.printStackTrace();
        }
        return packet;
    }

    public void stopConnection() throws IOException {
        outputStream.close();
        inputStream.close();
        clientSocket.close();
        running = false;
    }

    @Override
    public void run() {
        // message listener
        while(running){
            try {
                Object obj = inputStream.readObject();
                if(!(obj instanceof Packet)) {System.out.println("Object : " + obj);continue;}
                Packet response = (Packet)obj;
                System.out.println("Site id: " + this.initiatingSite);
                if(response.getMessage() == Packet.MESSAGES.SHUT_DOWN)break;
                else if(response.getMessage() == Packet.MESSAGES.VALIDATE){
                    System.out.println("On site : " + clientTM.getSiteId() + " adding transaction : " + response.getTransaction() + " to Validation Queue ");
                    clientTM.getClock().updateTime((int) response.getTime());
                    clientTM.addToValidationQueue(response.getTransaction());
                }
                else if(response.getMessage() == Packet.MESSAGES.ABORT){
                    System.out.println("On site : " + clientTM.getSiteId() + " aborting transaction " + response.getTransaction());
                    clientTM.getClock().updateTime((int) response.getTime());
                    clientTM.abortTransaction(response.getTransaction());
                    if(clientTM.getSiteId() == response.getTransaction().getInitiatingSite()){
                        // restart transaction
                        Transaction t = response.getTransaction();
                        System.out.println("On site : " + clientTM.getSiteId() + "Transaction with id: " + t.getTransactionId() + " aborted during global validation at TS: "+ clientTM.getClock().getTime());
                        Transaction restartAbortedTransaction = new Transaction(clientTM.getSiteId(), clientTM.getClock().getTime());
                        System.out.println("On site : " + clientTM.getSiteId() + "Transaction restarted with TID: " + restartAbortedTransaction.getTransactionId());
                        restartAbortedTransaction.setReadSet(t.getReadSet());
                        restartAbortedTransaction.setWriteSet(t.getWriteSet());
                        clientTM.updateWriteSet(restartAbortedTransaction);
                    }
                }
                else if(response.getMessage() == Packet.MESSAGES.GLOBAL_COMMIT){
                    System.out.println("On site : " + clientTM.getSiteId() + "Global Commit the transaction : " + response.getTransaction());
                    clientTM.getClock().updateTime((int) response.getTime());
                    clientTM.globalCommit(response.getTransaction());
                }
            }
//            catch(StreamCorruptedException sce){
//                System.out.println("Stream Corrupted Exception");
//                continue;
//            }
            catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
