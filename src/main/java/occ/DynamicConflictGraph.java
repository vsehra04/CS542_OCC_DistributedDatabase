package occ;

import java.util.*;

public class DynamicConflictGraph {
    private Map<Transaction, List<Transaction>> adjNodes;

    DynamicConflictGraph(){
        this.adjNodes = new HashMap<>();
    }

    public void addNode(Transaction t){
        adjNodes.put(t, new ArrayList<>());
    }

    public void removeNode(Transaction t){
        adjNodes.values().stream().forEach(e -> e.remove(t));
        adjNodes.remove(t);
    }

    public void addEdge(Transaction t1, Transaction t2){
        adjNodes.get(t1).add(t2);
    }

    public void checkConflict(Transaction t1, Transaction t2){
        Set<List<Integer>> t1_intersection = t1.getReadSet();
        t1_intersection.retainAll(t2.getWriteSet().keySet());

        Set<List<Integer>> t2_intersection = t2.getReadSet();
        t2_intersection.retainAll(t1.getWriteSet().keySet());

        Set<List<Integer>> write_intersection = t1.getWriteSet().keySet();
        write_intersection.retainAll(t2.getWriteSet().keySet());

        //WS(T1) conflicts WS(T2) --> Abort
        if(!write_intersection.isEmpty()){
            //abort
            abortTransaction(t1);
            removeNode(t1);
            System.out.println("Write Conflict");
            return;
        }

        //If T1 -> T2 and T2 -> T1 -> Abort
        else if(!t1_intersection.isEmpty() && !t2_intersection.isEmpty()){
            //abort
            removeNode(t1);
            abortTransaction(t1);
            System.out.println("Two way conflict");
            return;
        }

        //RS(T1) conflicts WS(T2) T1 -> T2
        else if(!t1_intersection.isEmpty()){
            addEdge(t1, t2);
            System.out.println("RS(T1) conflicts WS(T2) T1 -> T2");
        }

        //WS(T1) conflicts RS(T2) T2 -> T1
        else if(!t2_intersection.isEmpty()){
            addEdge(t2, t1);
            System.out.println("WS(T1) conflicts RS(T2) T2 -> T1");
        }

    }

    private void abortTransaction(Transaction t1) {
        t1.setState(Transaction.STATES.ABORTED);
    }

    public boolean checkCycle(Transaction t1){
        Set<Transaction> inStack = new HashSet<>();
        return dfs(t1, inStack);
    }

    private boolean dfs(Transaction t1, Set<Transaction> inStack) {
        inStack.add(t1);
        for(Transaction t: this.adjNodes.get(t1)){
            if(inStack.contains(t))return true;
            if(dfs(t, inStack))return true;
        }
        inStack.remove(t1);
        return false;
    }

    public boolean validateTransaction(Transaction validationTrans) {
//        System.out.println("here");
        // check conflicts with all overlapping transactions
        return validationTrans.getState() != Transaction.STATES.ABORTED;
    }

//    public static void main(String[] args){
//        Transaction t1 = new Transaction(1);
//        Transaction t2 = new Transaction(1);
//
//        DynamicConflictGraph dcg = new DynamicConflictGraph();
//        dcg.addNode(t1);
//        dcg.addNode(t2);
//
//       t1.appendToReadSet(Arrays.asList(1, 2));
//       t1.appendToReadSet(Arrays.asList(2, 3));
//       t1.appendToReadSet(Arrays.asList(4, 5));
//
//        t2.appendToReadSet(Arrays.asList(1, 2));
//        t2.appendToReadSet(Arrays.asList(3, 4));
//        t2.appendToReadSet(Arrays.asList(4, 5));
//
//        t1.appendToWriteSet(Arrays.asList(6, 5), 2);
//        t1.appendToWriteSet(Arrays.asList(1, 2), 2);
//        t1.appendToWriteSet(Arrays.asList(4, 5), 2);
//
//        t2.appendToWriteSet(Arrays.asList(16, 15), 2);
//        t2.appendToWriteSet(Arrays.asList(11, 12), 2);
//        t2.appendToWriteSet(Arrays.asList(14, 15), 2);
//
//
//
//        dcg.checkConflict(t1, t2);
//        dcg.adjNodes.forEach((key, value) -> System.out.println(key.getTransactionId() + ":" + value));
//    }

}
