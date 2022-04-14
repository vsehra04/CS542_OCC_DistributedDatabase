package occ;

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

        s1.setupServerClient(Arrays.asList("172.27.201.130","172.27.201.130","172.27.201.130"), Arrays.asList(5701, 5702, 5703));


        s2.setupServerClient(Arrays.asList("172.27.201.130","172.27.201.130","172.27.201.130"), Arrays.asList(5700, 5702, 5703));


        s3.setupServerClient(Arrays.asList("172.27.201.130","172.27.201.130","172.27.201.130"), Arrays.asList(5701, 5700, 5703));


        s4.setupServerClient(Arrays.asList("172.27.201.130","172.27.201.130","172.27.201.130"), Arrays.asList(5701, 5702, 5700));


        String t1 = "begin;read(1000,1);wait(1000);read(20,100);write(10,100,10);write(20,100,10);write(30,100,10);";
        s1.QueueTransaction(t1);

        Site.pause(30000);

        s1.transactionsDone(s1);
        s2.transactionsDone(s2);
        s3.transactionsDone(s3);
        s4.transactionsDone(s4);

//        while(true)continue;
    }

}
