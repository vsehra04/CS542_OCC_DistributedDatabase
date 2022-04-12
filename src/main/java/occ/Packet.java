package occ;

import java.io.Serializable;

public class Packet implements Serializable {

    public enum MESSAGES {ABORT, ACK, SHUT_DOWN, GLOBAL_COMMIT, VALIDATE}

    private long time;
    private Transaction transaction;
    private MESSAGES message;
    private int siteId;

    public Packet() {}

    public Packet(Packet p){
        this.time = p.getTime();
        this.transaction = p.getTransaction();
        this.message = p.getMessage();
        this.siteId = p.getSiteId();
    }

    public Packet(long time, Transaction transaction, MESSAGES message, int siteId) {
        this.time = time;
        this.transaction = transaction;
        this.message = message;
        this.siteId = siteId;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    public MESSAGES getMessage() {
        return message;
    }

    public void setMessage(MESSAGES message) {
        this.message = message;
    }

    public int getSiteId() {
        return siteId;
    }

    public void setSiteId(int siteId) {
        this.siteId = siteId;
    }
}
