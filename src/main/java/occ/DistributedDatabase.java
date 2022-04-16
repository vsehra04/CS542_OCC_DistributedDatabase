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

        int server1 = 5700, server2 = 5701, server3 = 5702, server4 = 5703;

        s1.setupServerClient(Arrays.asList("127.0.0.1","127.0.0.1","127.0.0.1"), Arrays.asList(server2, server3, server4));


        s2.setupServerClient(Arrays.asList("127.0.0.1","127.0.0.1","127.0.0.1"), Arrays.asList(server1, server3, server4));


        s3.setupServerClient(Arrays.asList("127.0.0.1","127.0.0.1","127.0.0.1"), Arrays.asList(server1, server2, server4));


        s4.setupServerClient(Arrays.asList("127.0.0.1","127.0.0.1","127.0.0.1"), Arrays.asList(server1, server2, server3));


//        String t1 = "begin;read(1000,1);wait(1000);read(20,100);write(10,100,10);write(20,100,10);write(30,100,10);";
//        s1.QueueTransaction(t1);
//
//        String t2 = "begin;read(2000,2);wait(2000);read(20,100);write(30,100,100)";
//        s2.QueueTransaction(t2);
//
//        String t3 = "begin;write(1000,1,10);wait(1010);read(20,100);";
//        s3.QueueTransaction(t3);

//        String t2 = "begin;read(2000,1);wait(1000);read(20,200);write(20,100,10);write(70,100,10);write(40,100,10);";
//        s1.QueueTransaction(t2);


//        String t3 = "begin;read(2000,2);wait(1010);read(20,100);write(30,100,100)";
//        s2.QueueTransaction(t3);

        String t1 = "begin;read(1000,1);wait(1000);read(20,100);write(10,100,10);write(20,100,10);write(30,100,10);";
        s1.QueueTransaction(t1);

        String t2 = "begin;read(2000,2);wait(1000);read(20,100);write(1,2,100)";
        s2.QueueTransaction(t2);
        String t3 = "begin;read(3000,3);wait(1000);write(1000,1,30);read(1,2)";
        s3.QueueTransaction(t3);

        String t4 = "begin;read(2000,1);wait(1000);read(30,100);write(20,100,10);write(1,100,10);";
        s1.QueueTransaction(t4);

        String t5 = "begin;read(2000,2);wait(1000);read(20,100);write(1,2,100)";
        s4.QueueTransaction(t5);

        String t6 = "begin;read(20,21);wait(2000);read(20,100);write(1,2,100)";
        s1.QueueTransaction(t6);

        Site.pause(60000);

        s1.transactionsDone(s1);
        s2.transactionsDone(s2);
        s3.transactionsDone(s3);
        s4.transactionsDone(s4);

//        while(true)continue;
    }

}
