package occ;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Site implements Runnable{
    final private int siteID;
    public volatile boolean running = true;

    //Main Datastore : Same on all sites (replicated database)
    private Database database;

    private Queue<String> transactionQueue = new ConcurrentLinkedQueue<String>();
    private final TransactionManager tm;
    private LamportClock clock;
    private final MultiServer server;
    private Map<Integer, Client> clientList;


    public Queue<String> getTransactionQueue() {
        return transactionQueue;
    }

    //Constructor
    public Site(int siteID, int numTables, int numRecords, int port){
        this.siteID = siteID;
        this.database = new Database(numTables, numRecords);
        this.tm = new TransactionManager(siteID, this.database);
        this.clock = new LamportClock(0);
        this.server = new MultiServer(siteID, tm, port);
    }

    //Fetch the SiteID
    public int getSiteID() {
        return siteID;
    }

    public Database getDatabase() {
        return database;
    }

    //Used to enqueue new transactions inorder to send them to Transaction Manager
    public void QueueTransaction(String t){
        clock.tick();
        this.transactionQueue.add(t);
    }

    //Thread to check whether the transaction queue on site is empty
    public void run() {
        while (running) {
            if(!transactionQueue.isEmpty()) {
                clock.tick();
                tm.getTransaction(transactionQueue.poll(), database.getDb());
            }
        }
    }

    //To stop queue checker thread
    public void stop(){
        while (!transactionQueue.isEmpty())continue;
        running = false;
    }

    //Pause the main thread for 1s
    public static void pause(long timeout){
        long Time0 = System.currentTimeMillis();
        long Time1;
        long runTime = 0;
        while (runTime < timeout) { // 1000 milliseconds or 1 second
            Time1 = System.currentTimeMillis();
            runTime = Time1 - Time0;
        }
    }

    void startThread(){
        Thread thread = new Thread(this, "Queue Checker");
        thread.start();
    }

    void transactionsDone(Site s){
        s.stop();
        tm.stop();
        try {
            server.stop();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void startServer(){
        try {
            server.startServer();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    Map<Integer, Client> connectClientToServers(List<String> ipList, List<Integer> portList){
        Map<Integer, Client> clientMap = new HashMap<>();
        List<Integer> serverList = new ArrayList<>();

        for(int i=1;i<=4;i++){
            if(i != this.siteID)serverList.add(i);
        }

        for(int i=0;i<3;i++){
            Client c = new Client(ipList.get(i), portList.get(i), serverList.get(i), this.siteID, this.tm);
            c.startConnection();
            clientMap.put(serverList.get(i), c);
        }

        return clientMap;
    }

    public void setClientList(Map<Integer, Client> clientList) {
        this.clientList = clientList;
    }
    public void startTMOperations(){
        this.tm.startValidationThread(this.clock);
        tm.setClientMap(this.clientList);
        tm.setServer(this.server);
    }

    public void setupServerClient(List<String> ipList, List<Integer> portList){
        setClientList(connectClientToServers(ipList, portList));
        startTMOperations();
        startThread();
    }
}
