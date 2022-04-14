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

    //Constructor
    public Site(int siteID, int numTables, int numRecords, int port){
        this.siteID = siteID;
//        this.database = getRandomArray(numTables, numRecords);
        this.database = new Database(numTables, numRecords);
        this.tm = new TransactionManager(siteID, this.database);
        this.clock = new LamportClock(0);
//        this.client = new Client(ip, port, siteID, tm);
        this.server = new MultiServer(siteID, tm, port);
//        tm.startValidationThread(clock);
    }

    //Fetch the SiteID
    public int getSiteID() {
        return siteID;
    }

    //Used to enqueue new transactions inorder to send them to Transaction Manager
    public void QueueTransaction(String t, Queue<String> transactionQueue){
        clock.tick();
        transactionQueue.add(t);
//        System.out.println(t);
    }

    //Thread to check whether the transaction queue on site is empty
    public void run() {
        while (running) {
            if(!transactionQueue.isEmpty()) {
//                System.out.println("top: " + transactionQueue.peek());
//                System.out.println("current trans: " + transactionQueue.peek());
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
    public static void pause(){
        long Time0 = System.currentTimeMillis();
        long Time1;
        long runTime = 0;
        while (runTime < 3999) { // 1000 milliseconds or 1 second
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
        System.out.println("Site Time:" + clock.getTime());
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

        for(int i=0;i<clientList.size();i++){
            Client c = new Client(ipList.get(i), portList.get(i), i+1, this.siteID, this.tm);
            c.startConnection();
            clientMap.put(i+1, c);
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

    public void setupServerClient(Site s, List<String> ipList, List<Integer> portList){
        s.startServer();
        s.setClientList(s.connectClientToServers(ipList, portList));
        s.startTMOperations();
        s.startThread();
    }

    public static void main(String[] args){
        Site s = new Site(1, 10000, 10000, 5700);
        // to do: write server.start() which will listen for connections -> this probably has to be done in a new thread
        // for client -> write connectToServer which will keep on trying to connect to server until it gets connected
//        s.startServer();
//        s.setClientList(s.connectClientToServers(Arrays.asList("1", "2", "3"), Arrays.asList(1,2,3)));
        // once all the clients are connected to different servers start tm thread and send tm all these values
//        s.startTMOperations();
//        s.startThread();

        s.setupServerClient(s, Arrays.asList("1", "2", "3"), Arrays.asList(1,2,3));


        String t1 = "begin;read(1000,1);wait(1000);read(20,100);write(10,100,10);write(20,100,10);write(30,100,10);";
        s.QueueTransaction(t1, s.transactionQueue);

        String t2 = "begin;read(2000,2);wait(2000);read(20,100);write(1,2,100)";
        s.QueueTransaction(t2, s.transactionQueue);
        String t3 = "begin;read(3000,3);wait(3000);write(1000,1,30);read(1,2)";
        s.QueueTransaction(t3, s.transactionQueue);

        String t4 = "begin;read(4000,4);wait(3020);write(1000,1,30)";
        s.QueueTransaction(t4, s.transactionQueue);

        s.pause();
//

//
//        String t5 = "begin;read(1,1);fail();write(1,1,56)";
//        s.QueueTransaction(t5, s.transactionQueue);

        s.transactionsDone(s);

    }

}
