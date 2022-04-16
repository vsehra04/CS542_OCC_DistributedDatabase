package occ;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TransactionManager{

    private DynamicConflictGraph dcg;

    public volatile boolean running = true;


    private final int siteId;
    private Queue<Transaction> validationQueue = new ConcurrentLinkedQueue<Transaction>();
    // current running transactions
    private Set<Transaction> currentTransactions; // Don't think thread-safe needed (confirm later)
    private Set<Transaction> committedTransactions;
    private Map<UUID, AtomicInteger> semiCommittedTransactions;
    private Map<UUID, Transaction> transactionIdMap;
    private Set<UUID> abortSet;
    private AtomicInteger activeThreads;
    private LamportClock clock;
    private Database database;
    private Map<Integer, Client> clientMap;
    private MultiServer server;

    public int getSiteId() {
        return siteId;
    }
    public TransactionManager(int siteId, Database db){
        this.siteId = siteId;
        currentTransactions = ConcurrentHashMap.newKeySet();
        semiCommittedTransactions = new ConcurrentHashMap<>();
        transactionIdMap = new HashMap<>();
        committedTransactions = ConcurrentHashMap.newKeySet();
        activeThreads = new AtomicInteger(0);
        abortSet = ConcurrentHashMap.newKeySet();
        this.database = db;
    }
    public int incrementAndGetSemiCommittedTransactions(Transaction transaction){
        //semiCommittedTransactions.put(transaction.getTransactionId(), (semiCommittedTransactions.get(transaction.getTransactionId()).getAndIncrement()));
        if(!semiCommittedTransactions.containsKey(transaction.getTransactionId()))return 0;
        //System.out.println("Site ID : " + siteId + " getting : " + semiCommittedTransactions.get(transaction.getTransactionId()).get());
        semiCommittedTransactions.getOrDefault(transaction.getTransactionId(), new AtomicInteger(0)).getAndIncrement();
        //System.out.println("Site ID : " + siteId + " after incrementing : " + semiCommittedTransactions.get(transaction.getTransactionId()).get());
        return semiCommittedTransactions.get(transaction.getTransactionId()).get();
    }
    public void setClientMap(Map<Integer, Client> clientMap) {
        this.clientMap = clientMap;
    }

    public void setServer(MultiServer server) {
        this.server = server;
    }

    public LamportClock getClock() {
        return clock;
    }

    public void getTransaction(String transaction, ArrayList<ArrayList<Integer>> database){
        Thread thread = new Thread(new Runnable() {
            public void run() {
                activeThreads.getAndIncrement();
                Transaction currTransaction = convertToTransaction(transaction, database);
                clock.tick();
                if(currTransaction != null && !Objects.equals(currTransaction.getState(), Transaction.STATES.ABORTED)){
//                    System.out.println(currTransaction.getState());
                    currentTransactions.add(currTransaction);
                    System.out.println("Transaction with TID: " + currTransaction.getTransactionId() + " on site ID : " + siteId + " added to validation queue");
                    validationQueue.add(currTransaction);
                }
                else{
                    if(currTransaction == null) System.out.println("Transaction aborted.");
                    else System.out.println("Transaction with Transaction Id: " + currTransaction.getTransactionId() + " on site ID : " + siteId + " aborted.");
                }
                activeThreads.decrementAndGet();
            }
        });
        thread.start();
    }

    public void startValidationThread(LamportClock siteClock){
        this.clock = siteClock;
        this.dcg = new DynamicConflictGraph(this.clock);

        Thread validation_thread = new Thread(new Runnable() {
            public void run() {
//                activeThreads.getAndIncrement();
                while(running){
                    if(!validationQueue.isEmpty()){
                        Transaction validationTrans = validationQueue.poll();
                        System.out.println(validationTrans.getState());
                        // call dcg function to check if valid -> if false -> we abort restart the transaction, else put in semi-committed state
                        clock.tick();
                        //Transaction present in abort set
                        if(abortSet.contains(validationTrans.getTransactionId())){ abortSet.remove(validationTrans.getTransactionId()); continue;}
                        if(dcg.validateTransaction(validationTrans)){
                            transactionIdMap.put(validationTrans.getTransactionId(), validationTrans);
                            semiCommittedTransactions.put(validationTrans.getTransactionId(), semiCommittedTransactions.getOrDefault(validationTrans.getTransactionId(), new AtomicInteger(0)));
                            validationTrans.setState(Transaction.STATES.SEMI_COMMITTED);

                            validationTrans.setEndTimeStamp(clock.getTime());
                            //System.out.println("End TS: " + validationTrans.getEndTimeStamp());
                            if(validationTrans.getInitiatingSite() != siteId){
                                System.out.println("Sie " + siteId + " Sending ack to initiating site : " + validationTrans.getInitiatingSite() + "for transaction " + validationTrans.getTransactionId());
                                Client client = clientMap.get(validationTrans.getInitiatingSite());
                                client.sendMessage(new Packet(clock.getTime(), validationTrans, Packet.MESSAGES.ACK, siteId));
                            }
                            else{
                                //Send all for validation
                                server.sendAll(Packet.MESSAGES.VALIDATE, validationTrans);
                            }
                        }
                        else{
                            System.out.println("Transaction " + validationTrans.getTransactionId() + " is in aborted state on site " + siteId);
                            // if the transaction's siteId is different, we will send an abort message to the initial site
                            if(validationTrans.getInitiatingSite() != siteId){
                                System.out.println("Sending abort to all sites");
                                server.sendAll(Packet.MESSAGES.ABORT, validationTrans);
                                // send abort to site with the given site id
                            }
                            else{
                                // local abort and restart
                                System.out.println("On site : " + siteId + " Transaction with id: " + validationTrans.getTransactionId() + " aborted during validation due to conflicts at TS: "+ clock.getTime());
                                Transaction restartAbortedTransaction = new Transaction(siteId, clock.getTime());
                                System.out.println("Transaction restarted with TID: " + restartAbortedTransaction.getTransactionId() + "on site : " + siteId);
                                restartAbortedTransaction.setReadSet(validationTrans.getReadSet());
                                restartAbortedTransaction.setWriteSet(validationTrans.getWriteSet());
                                updateWriteSet(restartAbortedTransaction);
                            }

                        }
                        currentTransactions.remove(validationTrans);
                    }
                }
//                activeThreads.getAndDecrement();
            }
        });
        validation_thread.start();

    }

    public void updateWriteSet(Transaction transaction) {
        ArrayList<ArrayList<Integer>> db = this.database.getDb();
        Map<List<Integer>, Integer> writeSet = transaction.getWriteSet();

        for(List<Integer> li: writeSet.keySet()){
            writeSet.replace(li, db.get(li.get(0)).get(li.get(1)));
        }
        transaction.setWriteSet(writeSet);
        clock.tick();
        currentTransactions.add(transaction);
        System.out.println("Transaction with TID: " + transaction.getTransactionId() + " added to validation queue on site " + siteId);
        validationQueue.add(transaction);
    }

    public void stop(){
        System.out.println("In stop on site : " + siteId);
//        while(!validationQueue.isEmpty())continue;
        while(activeThreads.get() != 0)continue;
//        System.out.println(validationQueue.poll());
        System.out.println("TM validation thread Stopped");
        System.out.println("TM Time:" + clock.getTime());
        running = false;
    }


    private boolean performIntegrityCheck(int row, int col, ArrayList<ArrayList<Integer>> database){
        int n = database.size();
        int m = database.get(0).size();

        return row < n && col < m;
    }

    // to do: add integrity check to see if the indices of read and write set are in range
    public Transaction convertToTransaction(String transaction, ArrayList<ArrayList<Integer>> database){

        String[] commands = transaction.split(";");

//        Transaction t = new Transaction(siteId);
        clock.tick();
        Transaction t = new Transaction(siteId, clock.getTime());
        System.out.println("New Transaction with ID: " + t.getTransactionId() + " started on site : " + siteId);
        System.out.println(transaction);
        boolean transactionStarted = false;
        for(String command: commands){
            if(command.equals("begin")){
                transactionStarted = true;
            }
            else{
                if(!transactionStarted){
                    System.out.println("Wrong transaction syntax. (Begin statement missing)");
                }
                else{
                    Pattern patternRow = Pattern.compile("\\([0-9]+", Pattern.CASE_INSENSITIVE);

                    if(command.startsWith("read")){
                        Pattern patternCol = Pattern.compile(",[0-9]+", Pattern.CASE_INSENSITIVE);
                        Matcher matcherRow = patternRow.matcher(command);
                        Matcher matcherCol = patternCol.matcher(command);
                        int row, col;
                        if(matcherRow.find() && matcherCol.find()){
                            row = Integer.parseInt(matcherRow.group(0).substring(1));
                            col = Integer.parseInt(matcherCol.group(0).substring(1));
                            if(!performIntegrityCheck(row, col, database)){
                                System.out.println("Integrity Constraint Violation");
                            }
                            else t.appendToReadSet(Arrays.asList(row,col));
//                            System.out.println(row + " " + col);
                        }
                        else{
                            System.out.println("Wrong Read syntax");
                        }

                    }
                    else if(command.startsWith("write")){
                        Pattern patternCol = Pattern.compile(",[0-9]+,", Pattern.CASE_INSENSITIVE);
                        Pattern patternVal = Pattern.compile(",[0-9]+\\)", Pattern.CASE_INSENSITIVE);
                        Matcher matcherRow = patternRow.matcher(command);
                        Matcher matcherCol = patternCol.matcher(command);
                        Matcher matcherVal = patternVal.matcher(command);
                        int row, col, value;
                        if(matcherRow.find() && matcherCol.find() && matcherVal.find()){
                            row = Integer.parseInt(matcherRow.group(0).substring(1));
                            col = Integer.parseInt(matcherCol.group(0).substring(1,matcherCol.group(0).length()-1));
                            value = Integer.parseInt(matcherVal.group(0).substring(1,matcherVal.group(0).length()-1));

                            if(!performIntegrityCheck(row, col, database)){
                                System.out.println("Integrity Constraint Violation");
                            }

                            t.appendToWriteSet(Arrays.asList(row,col),value);
//                            System.out.println(row + " " + col + " " + value);
                        }
                        else{
                            System.out.println("Wrong Write syntax");
                        }
                    }
                    else if(command.startsWith("wait")){
                        Pattern patternWait = Pattern.compile("[0-9]+", Pattern.CASE_INSENSITIVE);
                        Matcher matcher = patternWait.matcher(command);
                        int waitTime = 0;
                        if(matcher.find()){
                            waitTime = Integer.parseInt(matcher.group(0));
                        }
//                        System.out.println(waitTime);
                        try {
                            Thread.sleep(waitTime);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    else if(command.startsWith("fail")){
                        t.setState(Transaction.STATES.ABORTED);
                        System.out.println("Transaction Failed");
                        return t;
                    }
                }
            }
        }
        return t;
    }

    public void addToValidationQueue(Transaction t){
        validationQueue.add(t);
    }

    public void abortTransaction(Transaction t){
        clock.tick();
        if(semiCommittedTransactions.containsKey(t.getTransactionId())){
            System.out.println("Removing transactions " + t.getTransactionId() + " from semi-committed transactions on site : " + siteId);
            semiCommittedTransactions.remove(t.getTransactionId());
            transactionIdMap.remove(t.getTransactionId());
            dcg.removeTransaction(t);
        }
        else{
            System.out.println("Adding transaction : " + t.getTransactionId() + "to abort set on site : " + siteId);
            abortSet.add(t.getTransactionId());
        }

//        if(t.getInitiatingSite() == this.siteId){
//            System.out.println("Transaction with id: " + t.getTransactionId() + " aborted during global validation due to conflicts at TS: "+ clock.getTime());
//            Transaction restartAbortedTransaction = new Transaction(this.siteId, clock.getTime());
//            System.out.println("Transaction restarted with TID: " + restartAbortedTransaction.getTransactionId());
//            restartAbortedTransaction.setReadSet(t.getReadSet());
//            restartAbortedTransaction.setWriteSet(t.getWriteSet());
//            updateWriteSet(restartAbortedTransaction);
//        }
    }

    public void globalCommit(Transaction t){
        System.out.println("Global Committing :" + t.getTransactionId() + " at site: " + this.siteId + " at time: " + this.clock.getTime());
        semiCommittedTransactions.remove(t.getTransactionId());
        transactionIdMap.remove(t.getTransactionId());
        committedTransactions.add(t);

        t.getWriteSet().forEach((key, val) -> database.setDbElement(key.get(0), key.get(1), val));
        clock.tick();
        t.setEndTimeStamp(clock.getTime());
        dcg.dcgNodes.remove(t.getTransactionId());
        dcg.dcgNodes.add(t.getTransactionId());
        System.out.println("Current Committed Transactions on site " + this.siteId + " : " + committedTransactions);
    }

//    public static void main(String[] args){
//        TransactionManager t = new TransactionManager(1);
////        t.convertToTransaction("begin;read(1321,2);wait(5000);write(21,42,245)");
//    }
}
