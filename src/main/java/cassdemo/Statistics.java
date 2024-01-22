package cassdemo;

public class Statistics {
    private int preemptionsCount;

    public Statistics(){
        preemptionsCount=0;
    }

    public synchronized void increasePreemptionCount(){
        ++preemptionsCount;
    }

    public int getPreemptionsCount() {
        return this.preemptionsCount;
    }

    @Override
    public String toString() {
        return "preemptionCount: "+preemptionsCount;
    }
}
