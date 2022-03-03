package occ;

import java.util.Set;

public class TransactionManager {

    private DynamicConflictGraph dcg;

    // current running transactions
    private Set<Transaction> currentTransactions; // Don't think thread-safe needed (confirm later)


}
