package occ;

import java.util.*;

public class DynamicConflictGraph {
    private Map<Transaction, List<Transaction>> adjNodes;

    //To store EndTime of Committed and Semi-Committed Transactions (Descending Order)
    ArrayList<Transaction> dcgNodes = new ArrayList<>();

    LamportClock lc = new LamportClock();

    DynamicConflictGraph(){
        this.adjNodes = new HashMap<>();
    }

    public void addNode(Transaction t){
        adjNodes.put(t, new ArrayList<>());
        lc.tick();
    }

    public void removeNode(Transaction t){
        adjNodes.values().stream().forEach(e -> e.remove(t));
        adjNodes.remove(t);
        lc.tick();
    }

    public void addEdge(Transaction t1, Transaction t2){
        adjNodes.get(t1).add(t2);
        lc.tick();
    }

    public void checkConflict(Transaction t1, Transaction t2){

        System.out.println(t1.getWriteSet() + "T2 : "  + t2.getWriteSet());
        Set<List<Integer>> t1_intersection = new HashSet<List<Integer>>(t1.getReadSet());
        t1_intersection.retainAll(t2.getWriteSet().keySet());

        Set<List<Integer>> t2_intersection = new HashSet<List<Integer>>(t2.getReadSet());
        t2_intersection.retainAll(t1.getWriteSet().keySet());

        Set<List<Integer>> write_intersection = new HashSet<List<Integer>>(t1.getWriteSet().keySet());
        write_intersection.retainAll(t2.getWriteSet().keySet());

        //WS(T1) conflicts WS(T2) --> Abort
        if(!write_intersection.isEmpty()){
            //abort
            removeNode(t1);
            System.out.println("Write Conflict");
        }

        //If T1 -> T2 and T2 -> T1 -> Abort
        else if(!t1_intersection.isEmpty() && !t2_intersection.isEmpty()){
            //abort
            removeNode(t1);
            System.out.println("Two way conflict");
        }

        //RS(T1) conflicts WS(T2) T1 -> T2
        else if(!t1_intersection.isEmpty()){
            addEdge(t1, t2);
            dcgNodes.add(t1);
            System.out.println("RS(T1) conflicts WS(T2) T1 -> T2");
        }

        //WS(T1) conflicts RS(T2) T2 -> T1
        else if(!t2_intersection.isEmpty()){
            addEdge(t2, t1);
            dcgNodes.add(t1);
            System.out.println("WS(T1) conflicts RS(T2) T2 -> T1");
        }

        lc.tick();
    }

    public void getConcurrentTransactions(Transaction t){
        System.out.println(t.getTransactionId());
        if(dcgNodes.isEmpty()){
            dcgNodes.add(t);
        }
        else {
           for(int i=dcgNodes.size()-1; i>=0; i--){
               if(dcgNodes.get(i).getEndTimeStamp() >= t.getStartTimestamp()){
                  //System.out.println("End Time" + dcgNodes.get(i).getEndTimeStamp());
                  // System.out.println("Check Conflict with : " + dcgNodes.get(i).getTransactionId() + "End Timestamp : " + dcgNodes.get(i).getEndTimeStamp());
                   checkConflict(t, dcgNodes.get(i));
               }
               else{
                   break;
               }
           }
        }
    }

//    public static void main(String[] args){
//        Transaction t1 = new Transaction(1, 0);
//        Transaction t2 = new Transaction(1, 0);
//        Transaction t3 = new Transaction(1, 0);
//
//        DynamicConflictGraph dcg = new DynamicConflictGraph();
//        dcg.addNode(t1);
//        dcg.addNode(t2);
//        dcg.addNode(t3);
//
//        System.out.println("t1 " + t1.getTransactionId());
//        System.out.println("t2 " + t2.getTransactionId());
//        System.out.println("t3 " + t3.getTransactionId());
//
//       t1.appendToReadSet(Arrays.asList(1, 2));
//       dcg.lc.tick();
//       t1.appendToReadSet(Arrays.asList(2, 3));
//        dcg.lc.tick();
//       t1.appendToReadSet(Arrays.asList(4, 5));
//        dcg.lc.tick();
//
//        t1.appendToWriteSet(Arrays.asList(6, 5), 2);
//        dcg.lc.tick();
//        t1.appendToWriteSet(Arrays.asList(1, 2), 2);
//        dcg.lc.tick();
//        t1.appendToWriteSet(Arrays.asList(4, 5), 2);
//        dcg.lc.tick();
//        t1.setEndTimeStamp(dcg.lc.getTime());
//
//        t2.appendToReadSet(Arrays.asList(1, 2));
//        dcg.lc.tick();
//        t2.appendToReadSet(Arrays.asList(3, 4));
//        dcg.lc.tick();
//        t2.appendToReadSet(Arrays.asList(4, 5));
//        dcg.lc.tick();
//
//        t2.appendToWriteSet(Arrays.asList(16, 15), 2);
//        dcg.lc.tick();
//        t2.appendToWriteSet(Arrays.asList(11, 12), 2);
//        dcg.lc.tick();
//        t2.appendToWriteSet(Arrays.asList(14, 15), 2);
//        dcg.lc.tick();
//        t2.setEndTimeStamp(dcg.lc.getTime());
//        System.out.println(t2.getWriteSet());
//
//
//
//        //Conflict with t3
//        dcg.lc.updateTime(8);
//        t3.appendToReadSet(Arrays.asList(21, 22));
//        dcg.lc.tick();
//        t3.appendToReadSet(Arrays.asList(23, 42));
//        dcg.lc.tick();
//        t3.appendToReadSet(Arrays.asList(24, 25));
//        dcg.lc.tick();
//
//        t3.appendToWriteSet(Arrays.asList(16, 15), 2);
//        dcg.lc.tick();
//        t3.appendToWriteSet(Arrays.asList(3, 4), 2);
//        dcg.lc.tick();
//        t3.appendToWriteSet(Arrays.asList(124, 125), 2);
//        dcg.lc.tick();
//        t3.setEndTimeStamp(dcg.lc.getTime());
//
//
//
//        //dcg.checkConflict(t1, t2);
//        //dcg.adjNodes.forEach((key, value) -> System.out.println(key.getTransactionId() + ":" + value));
//
//        System.out.println("T1");
//        dcg.getConcurrentTransactions(t1);
//        System.out.println("T2");
//        dcg.getConcurrentTransactions(t2);
//        System.out.println("T3");
//        dcg.getConcurrentTransactions(t3);
////        System.out.println(dcg.dcg_nodes_pq);
//        System.out.println("Final : " + t2.getWriteSet());
//        dcg.adjNodes.forEach((key, value) -> System.out.println(key.getTransactionId() + ":" + value));
//    }

}
