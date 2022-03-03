package occ;

import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;

public class Site {
    final private int siteID;

    //Main Datastore : Same on all sites (replicated database)
    private ArrayList<ArrayList<Integer>> database = new ArrayList<ArrayList<Integer>>();


    public Site(int siteID, int numTables, int numRecords){
        this.siteID = siteID;
        this.database = getRandomArray(numTables, numRecords);
    }

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

    public int getSiteID() {
        return siteID;
    }

}
