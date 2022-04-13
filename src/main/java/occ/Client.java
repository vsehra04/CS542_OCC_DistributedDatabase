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
                else if(response.getMessage() == Packet.MESSAGES.VALIDATE){
                    System.out.println("Add to Validation Queue");
                    clientTM.getClock().updateTime((int) response.getTime());
                    clientTM.addToValidationQueue(response.getTransaction());
                }
                else if(response.getMessage() == Packet.MESSAGES.ABORT){
                    System.out.println("Abort this transaction in my site");
                    clientTM.getClock().updateTime((int) response.getTime());
                    clientTM.abortTransaction(response.getTransaction());
                    if(clientTM.getSiteId() == response.getTransaction().getInitiatingSite()){
                        // restart transaction
                        Transaction t = response.getTransaction();
                        System.out.println("Transaction with id: " + t.getTransactionId() + " aborted during global validation at TS: "+ clientTM.getClock().getTime());
                        Transaction restartAbortedTransaction = new Transaction(clientTM.getSiteId(), clientTM.getClock().getTime());
                        System.out.println("Transaction restarted with TID: " + restartAbortedTransaction.getTransactionId());
                        restartAbortedTransaction.setReadSet(t.getReadSet());
                        restartAbortedTransaction.setWriteSet(t.getWriteSet());
                        clientTM.updateWriteSet(restartAbortedTransaction);
                    }
                }
                else if(response.getMessage() == Packet.MESSAGES.GLOBAL_COMMIT){
                    System.out.println("Global Commit the transaction");
                    clientTM.getClock().updateTime((int) response.getTime());
                    clientTM.globalCommit(response.getTransaction());
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
