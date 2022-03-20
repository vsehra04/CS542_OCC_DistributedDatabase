package occ;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Site implements Runnable{
    final private int siteID;
    public volatile boolean running = true;

    //Main Datastore : Same on all sites (replicated database)
    private ArrayList<ArrayList<Integer>> database;
    private Queue<String> transactionQueue = new ConcurrentLinkedQueue<String>();
    private final TransactionManager tm;

    //Constructor
    public Site(int siteID, int numTables, int numRecords){
        this.siteID = siteID;
        this.database = getRandomArray(numTables, numRecords);
        this.tm = new TransactionManager(siteID);
        tm.startValidationThread();
    }

    //Fetch the SiteID
    public int getSiteID() {
        return siteID;
    }

    //Generated a random database (2D)
    public ArrayList<ArrayList<Integer>> getRandomArray(int numTables, int numRecords){
        ArrayList<ArrayList<Integer>> arr = new ArrayList<ArrayList<Integer>>();
        for(int i=1; i<=numTables; i++) {
            Random rnd = new Random(i*50);
            ArrayList<Integer> temp = new ArrayList<Integer>();
            for (int j = 0; j < numRecords; j++) {
                temp.add(rnd.nextInt(100));
            }
            arr.add(temp);
        }
        return arr;
    }

    //Used to enqueue new transactions inorder to send them to Transaction Manager
    public void QueueTransaction(String t, Queue<String> transactionQueue){
        transactionQueue.add(t);
//        System.out.println(t);
    }

    //Thread to check whether the transaction queue on site is empty
    public void run() {
        while (running) {
            if(!transactionQueue.isEmpty()) {
//                System.out.println("top: " + transactionQueue.peek());
//                System.out.println("current trans: " + transactionQueue.peek());
                tm.getTransaction(transactionQueue.poll(), database);
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
    }

    public static void main(String[] args){
        Site s = new Site(1, 10000, 10000);
        s.startThread();


        String t1 = "begin;read(1000,1);wait(1000);write(10,100,10)";
        s.QueueTransaction(t1, s.transactionQueue);

        String t2 = "begin;read(2000,2);wait(2000);write(20,20,200)";
        s.QueueTransaction(t2, s.transactionQueue);
        String t3 = "begin;read(3000,3);wait(3000);write(30,30,30)";
        s.QueueTransaction(t3, s.transactionQueue);

        s.pause();

        String t4 = "begin;read(4000,4);wait(4000);write(40,40,40)";
        s.QueueTransaction(t4, s.transactionQueue);

        String t5 = "begin;read(1,1);fail();write(1,1,56)";
        s.QueueTransaction(t5, s.transactionQueue);

        s.transactionsDone(s);

    }

}
