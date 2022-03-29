package occ;

import java.util.concurrent.atomic.AtomicInteger;

public class LamportClock {

    private AtomicInteger time;

    public LamportClock(){
        this.time = new AtomicInteger(0);
    }

    public LamportClock(int time){
        this.time = new AtomicInteger(time);
    }

    public int tick(){
        return time.incrementAndGet();
    }

    public int getTime(){
        return time.get();
    }

    public int updateTime(int receivedTime){
        int currTime = time.get();

        int newTime = Integer.max(currTime, receivedTime);

        // In case we have the lesser time, we'll update
        time.compareAndSet(currTime, newTime);
//        time.compareAndSet(newTime, newTime+1);
        return time.incrementAndGet();
    }

    public static void main(String[] args){
        LamportClock clock = new LamportClock(0);

        System.out.println(clock.tick());
        System.out.println(clock.tick());
//        System.out.println(clock.updateTime(1));
    }

}
