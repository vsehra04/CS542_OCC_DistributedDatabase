package occ;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class DynamicConflictGraph {
    private Map<UUID, List<UUID>> adjNodes;
    Map<UUID, Transaction> UUID_Transaction_Map = new ConcurrentHashMap<>();
    //To store EndTime of Committed and Semi-Committed Transactions (Descending Order)
    CopyOnWriteArrayList<UUID> dcgNodes = new CopyOnWriteArrayList<>();

    private LamportClock lc;

    DynamicConflictGraph(LamportClock lc){
        this.adjNodes = new ConcurrentHashMap<>();
        this.lc = lc;
    }

    public void addNode(Transaction t){
        this.adjNodes.put(t.getTransactionId(), new ArrayList<>());
        UUID_Transaction_Map.put(t.getTransactionId(), t);
        lc.tick();
    }

    public void removeNode(Transaction t){
        adjNodes.values().stream().forEach(e -> e.remove(t.getTransactionId()));
        adjNodes.remove(t.getTransactionId());
        UUID_Transaction_Map.remove(t.getTransactionId());
        lc.tick();
    }

    public void addEdge(Transaction t1, Transaction t2){
        if(!adjNodes.containsKey(t1.getTransactionId()))addNode(t1); // not sure if correct or not
        adjNodes.get(t1.getTransactionId()).add(t2.getTransactionId());
        lc.tick();
    }

    public void checkConflict(Transaction t1, Transaction t2){
        Set<List<Integer>> t1_intersection = new HashSet<List<Integer>>(t1.getReadSet());
        t1_intersection.retainAll(t2.getWriteSet().keySet());

        Set<List<Integer>> t2_intersection = new HashSet<List<Integer>>(t2.getReadSet());
        t2_intersection.retainAll(t1.getWriteSet().keySet());

        Set<List<Integer>> write_intersection = new HashSet<List<Integer>>(t1.getWriteSet().keySet());
        write_intersection.retainAll(t2.getWriteSet().keySet());

        //WS(T1) conflicts WS(T2) --> Abort
        if(!write_intersection.isEmpty()){
            //abort
            abortTransaction(t1);
            System.out.println("Write Conflict by transaction " + t1.getTransactionId() + " and " + t2.getTransactionId());
            return;
        }

        //If T1 -> T2 and T2 -> T1 -> Abort
        else if(!t1_intersection.isEmpty() && !t2_intersection.isEmpty()){
            //abort
            abortTransaction(t1);
            System.out.println("Two way conflict by transaction " + t1.getTransactionId() + " and " + t2.getTransactionId());
            return;
        }
        //RS(T1) conflicts WS(T2) T1 -> T2
        else if(!t1_intersection.isEmpty()){
            addEdge(t1, t2);
        }

        //WS(T1) conflicts RS(T2) T2 -> T1
        else if(!t2_intersection.isEmpty()){
            addEdge(t2, t1);
        }
    }

    public void getConcurrentTransactions(Transaction t){
        if(dcgNodes.isEmpty()){
            return;
        }
        else {
            ListIterator<UUID> iterator = dcgNodes.listIterator(dcgNodes.size());
           while(iterator.hasPrevious()){
               UUID curr = iterator.previous();
               if(UUID_Transaction_Map.containsKey(curr) && UUID_Transaction_Map.get(curr).getEndTimeStamp() >= t.getStartTimestamp()){
                   if(UUID_Transaction_Map.containsKey(curr)) checkConflict(t, UUID_Transaction_Map.get(curr));
               }
               else{
                   break;
               }
           }
        }
        lc.tick();
    }


    private void abortTransaction(Transaction t1) {
        removeNode(t1);
        t1.setState(Transaction.STATES.ABORTED);
    }

    public boolean checkCycle(Transaction t1){
        Set<Transaction> inStack = new HashSet<>();
        Map<UUID, List<UUID>> adjacencyList = Map.copyOf(adjNodes);
        boolean cycle = dfs(t1, inStack, adjacencyList);
        if(cycle){
            System.out.println("Cycle caused by transaction " + t1.getTransactionId() + " detected ");
            abortTransaction(t1);
        }
        if(!cycle){
            dcgNodes.add(t1.getTransactionId());
            UUID_Transaction_Map.put(t1.getTransactionId(), t1);
        }
        return cycle;
    }

    private boolean dfs(Transaction t1, Set<Transaction> inStack, Map<UUID, List<UUID>> adjNodes) {
        if(t1 == null)return false;
        if(adjNodes.get(t1.getTransactionId()) == null)return false;
        inStack.add(t1);
        for(UUID u: adjNodes.get(t1.getTransactionId())){
            Transaction t = UUID_Transaction_Map.get(u);
            if(inStack.contains(t))return true;
            if(dfs(t, inStack, adjNodes))return true;
        }
        inStack.remove(t1);
        return false;
    }

    public boolean validateTransaction(Transaction validationTrans) {
        // check conflicts with all overlapping transactions
        addNode(validationTrans);
        getConcurrentTransactions(validationTrans);
        if(validationTrans.getState() == Transaction.STATES.ABORTED)return false;
        return !checkCycle(validationTrans);
    }

    public void removeTransaction(Transaction t){
        dcgNodes.remove(t.getTransactionId());
        UUID_Transaction_Map.remove(t.getTransactionId());
        removeNode(t);
    }

}
