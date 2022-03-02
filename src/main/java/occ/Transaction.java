package occ;

import java.util.*;

public class Transaction {

    // site where this transaction is generated
    final private int initiatingSite;
    final private UUID transactionId;

    // current state of the transaction
    private String state;
    // contain all the indices that are read
    private Set<Integer> readSet;
    // indices of write operations and
    private Map<Integer, Integer> writeSet;

    //transaction start timestamp
    final private long startTimestamp;
    // transaction timestamp when it gets semi-committed
    private long endTimeStamp;

    // list with all incoming edges in the conflict graph
    private Set<UUID> incomingEdges;

    public Transaction(int initiatingSite) {
        this.readSet = new HashSet<>();
        this.writeSet = new HashMap<>();
        this.startTimestamp = System.nanoTime();
        this.initiatingSite = initiatingSite;
        this.transactionId = UUID.randomUUID();
        this.state = "running";
        this.incomingEdges = new HashSet<>();
    }

    public int getInitiatingSite() {
        return initiatingSite;
    }

    public UUID getTransactionId() {
        return transactionId;
    }

    public String getState() {
        return state;
    }

    public Set<Integer> getReadSet() {
        return readSet;
    }

    public Map<Integer, Integer> getWriteSet() {
        return writeSet;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public long getEndTimeStamp() {
        return endTimeStamp;
    }

    public Set<UUID> getIncomingEdges() {
        return incomingEdges;
    }

    public void setState(String state) {
        this.state = state;
    }

    public void setEndTimeStamp(long endTimeStamp) {
        this.endTimeStamp = endTimeStamp;
    }

    // Read set operations
    public void appendToReadSet(int index){
        this.readSet.add(index);
    }
    public void removeFromReadSet(int index){
        this.readSet.remove(index);
    }
    public boolean isPresentReadSet(int index){
        return this.readSet.contains(index);
    }

    // Write Set operations
    public void appendToWriteSet(int index, int value){
        this.writeSet.put(index, value);
    }
    public void removeFromWriteSet(int index){
        this.writeSet.remove(index);
    }
    public boolean isPresentWriteSet(int index){
        return this.writeSet.containsKey(index);
    }

    //incomingEDGES operations
    public void appendToIncomingEdges(UUID transactionId){
        this.incomingEdges.add(transactionId);
    }
    public void removeFromIncomingEdges(UUID transactionId){
        this.incomingEdges.remove(transactionId);
    }
    public boolean isPresentIncomingEdges(UUID transactionId){
        return this.incomingEdges.contains(transactionId);
    }

}
