package occ;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Site implements Runnable{
    final private int siteID;
    public volatile boolean running = true;

    //Main Datastore : Same on all sites (replicated database)
    private ArrayList<ArrayList<Integer>> database = new ArrayList<ArrayList<Integer>>();
    private Queue<String> transactionQueue = new ConcurrentLinkedQueue<String>();
    private TransactionManager tm;

    //Constructor
    public Site(int siteID, int numTables, int numRecords){
        this.siteID = siteID;
        this.database = getRandomArray(numTables, numRecords);
        this.tm = new TransactionManager(1);
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
    }

    //Thread to check whether the transaction queue on site is empty
    public void run() {
        Site s = new Site(1, 100, 100);
        while (running) {
            if(!transactionQueue.isEmpty()) {
                //System.out.println(transactionQueue.poll());
                tm.getTransaction(transactionQueue.poll());
            }
        }
    }

    //To stop queue checker thread
    public void stop(){
        running = false;
    }

    //Pause the main thread for 1s
    public static void pause(){
        long Time0 = System.currentTimeMillis();
        long Time1;
        long runTime = 0;
        while (runTime < 1000) { // 1000 milliseconds or 1 second
            Time1 = System.currentTimeMillis();
            runTime = Time1 - Time0;
        }
    }

    public static void main(String[] args){
        Site s = new Site(1, 100, 100);
        Thread thread = new Thread(s, "Queue_Checker");
        thread.start();


        String t1 = "t1";
        s.QueueTransaction(t1, s.transactionQueue);

        String t2 = "t2";
        s.QueueTransaction(t2, s.transactionQueue);
        String t3 = "t3";
        s.QueueTransaction(t3, s.transactionQueue);

        s.pause();

        String t4 = "t4";
        s.QueueTransaction(t4, s.transactionQueue);

        s.stop();
    }

}
