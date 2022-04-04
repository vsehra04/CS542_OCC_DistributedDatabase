package occ;

import java.util.ArrayList;
import java.util.Random;

public class Database {

    private ArrayList<ArrayList<Integer>> db;

    public Database(int numTables, int numRecords){
        this.db = getRandomArray(numTables, numRecords);
    }

    //Generated a random database (2D)
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

    public ArrayList<ArrayList<Integer>> getDb() {
        return db;
    }

    public void setDb(ArrayList<ArrayList<Integer>> db) {
        this.db = db;
    }

    public void setDbElement(int row, int col, int val){
        db.get(row).set(col, val);
    }

}
