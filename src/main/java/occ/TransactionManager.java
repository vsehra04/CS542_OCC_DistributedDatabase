package occ;

import java.util.Arrays;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TransactionManager {

    private DynamicConflictGraph dcg;
    private int siteId;
    // current running transactions
    private Set<Transaction> currentTransactions; // Don't think thread-safe needed (confirm later)


    public void getTransaction(String t){
        System.out.println(t);
    }
    public TransactionManager(int siteId){
        this.siteId = siteId;
    }

    // to do: add integrity check to see if the indices of read and write set are in range
    public void convertToTransaction(String transaction){

        String[] commands = transaction.split(";");

        Transaction t = new Transaction(siteId);
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
                            t.appendToReadSet(Arrays.asList(row,col));
                            System.out.println(row + " " + col);
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
                            col = Integer.parseInt(matcherCol.group(0).substring(1,matcherCol.group(0).length()-2));
                            value = Integer.parseInt(matcherVal.group(0).substring(1,matcherVal.group(0).length()-2));

                            t.appendToWriteSet(Arrays.asList(row,col),value);
                            System.out.println(row + " " + col + " " + value);
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
                        System.out.println(waitTime);
                        try {
                            Thread.sleep(waitTime);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    else if(command.startsWith("fail")){
                        System.out.println("Transaction Failed");
                    }
                }
            }
        }

    }

    public static void main(String[] args){
        TransactionManager t = new TransactionManager(1);
        t.convertToTransaction("begin;read(1321,2);wait(5000);write(21,42,245)");
    }
}
