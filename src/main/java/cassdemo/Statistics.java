package cassdemo;

public class Statistics {
    private int preemptionsCount;
    private int backendExceptionCount;

    public Statistics(){
        preemptionsCount=0;
    }

    public synchronized void increasePreemptionCount(){
        ++preemptionsCount;
    }

    public synchronized void increaseBackendExcepionCount(){
        ++backendExceptionCount;
    }

    public int getPreemptionsCount() {
        return this.preemptionsCount;
    }

    public int getBackendExceptionCount() {
        return this.backendExceptionCount;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("preemptionCount:\t"+preemptionsCount+"\n");
        sb.append("backenExceptionCount:\t"+backendExceptionCount+"\n");
        return sb.toString();
    }
}
