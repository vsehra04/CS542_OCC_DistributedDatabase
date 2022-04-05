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
    private Set<Transaction> semiCommittedTransactions;
    private AtomicInteger activeThreads;
    private LamportClock clock;
    private Database database;
    public TransactionManager(int siteId, Database db){
        this.siteId = siteId;
        currentTransactions = ConcurrentHashMap.newKeySet();
        semiCommittedTransactions = new HashSet<>();
        committedTransactions = new HashSet<>();
        activeThreads = new AtomicInteger(0);
        this.database = db;
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
                    validationQueue.add(currTransaction);
                }
                else{
                    if(currTransaction == null) System.out.println("Transaction aborted.");
                    else System.out.println("Transaction with Transaction Id: " + currTransaction.getTransactionId() + " aborted.");
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
                while(running){
                    if(!validationQueue.isEmpty()){
                        Transaction validationTrans = validationQueue.poll();
                        System.out.println(validationTrans.getTransactionId());
                        // call dcg function to check if valid -> if false -> we abort restart the transaction, else put in semi-committed state
                        clock.tick();
                        if(dcg.validateTransaction(validationTrans)){
                            semiCommittedTransactions.add(validationTrans);
                            validationTrans.setState(Transaction.STATES.SEMI_COMMITTED);
                            // send this transaction to all sites for validation (will need a thread that monitors all the incoming messages)
                        }
                        else{
                            // if the transaction's siteId is different, we will send an abort message to the initial site
                            if(validationTrans.getInitiatingSite() != siteId){
                                continue;
                                // send abort to site with the given site id
                            }
                            else{
                                Transaction restartAbortedTransaction = new Transaction(siteId, clock.getTime());
                                updateWriteSet(restartAbortedTransaction);
                            }

                        }
                        currentTransactions.remove(validationTrans);
                    }
                }
            }
        });
        validation_thread.start();
    }

    private void updateWriteSet(Transaction transaction) {
        ArrayList<ArrayList<Integer>> db = this.database.getDb();
        Map<List<Integer>, Integer> writeSet = transaction.getWriteSet();

        for(List<Integer> li: writeSet.keySet()){
            writeSet.replace(li, db.get(li.get(0)).get(li.get(1)));
        }
        transaction.setWriteSet(writeSet);
        clock.tick();
        currentTransactions.add(transaction);
        validationQueue.add(transaction);
    }

    public void stop(){
        System.out.println("In stop");
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
        System.out.println("New Transaction with ID: " + t.getTransactionId() + " started");
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

//    public static void main(String[] args){
//        TransactionManager t = new TransactionManager(1);
////        t.convertToTransaction("begin;read(1321,2);wait(5000);write(21,42,245)");
//    }
}
