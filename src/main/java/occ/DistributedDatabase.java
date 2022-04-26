package occ;

import java.util.ArrayList;
import java.util.Arrays;

public class DistributedDatabase {

    public static void main(String[] args){

        Site s1 = new Site(1, 10000, 10000, 5700);
        Site s2 = new Site(2, 10000, 10000, 5701);
        Site s3 = new Site(3, 10000, 10000, 5702);
        Site s4 = new Site(4, 10000, 10000, 5703);

        s1.startServer();
        s2.startServer();
        s3.startServer();
        s4.startServer();

        Site.pause(2000);

        int server1 = 5700, server2 = 5701, server3 = 5702, server4 = 5703;

        s1.setupServerClient(Arrays.asList("127.0.0.1","127.0.0.1","127.0.0.1"), Arrays.asList(server2, server3, server4));


        s2.setupServerClient(Arrays.asList("127.0.0.1","127.0.0.1","127.0.0.1"), Arrays.asList(server1, server3, server4));


        s3.setupServerClient(Arrays.asList("127.0.0.1","127.0.0.1","127.0.0.1"), Arrays.asList(server1, server2, server4));


        s4.setupServerClient(Arrays.asList("127.0.0.1","127.0.0.1","127.0.0.1"), Arrays.asList(server1, server2, server3));

        String t1 = "begin;read(1000,1);wait(1000);read(20,100);write(10,100,10);write(20,100,10);write(30,100,10);";
        s1.QueueTransaction(t1);

        String t2 = "begin;read(1000,1);wait(2000);write(30,100,10);";
        s2.QueueTransaction(t2);


        Site.pause(100000);


        ArrayList<ArrayList<Integer>> s1_db =  s1.getDatabase().getDb();
        ArrayList<ArrayList<Integer>> s2_db =  s2.getDatabase().getDb();
        ArrayList<ArrayList<Integer>> s3_db =  s3.getDatabase().getDb();
        ArrayList<ArrayList<Integer>> s4_db =  s4.getDatabase().getDb();

        for(int i=0; i<10000; i++){
            for(int j=0; j<10000; j++){
               if(s1_db.get(i).get(j).equals(s2_db.get(i).get(j)) && s1_db.get(i).get(j).equals(s3_db.get(i).get(j)) && s1_db.get(i).get(j).equals(s4_db.get(i).get(j))){
                   continue;
               }
               else{
                   System.out.println("Not consistent");
               }
            }
        }
        System.out.println("Consistent");


        Site.pause(60000);

        s1.transactionsDone(s1);
        s2.transactionsDone(s2);
        s3.transactionsDone(s3);
        s4.transactionsDone(s4);

//        while(true)continue;
    }

}
