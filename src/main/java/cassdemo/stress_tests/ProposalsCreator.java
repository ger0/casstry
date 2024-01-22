package cassdemo.stress_tests;

import cassdemo.backend.BackendSession;

public class ProposalsCreator {
    private Thread[] threads;

    public ProposalsCreator(int firstListNumber, int lastListNumber, String listBaseName, int numberOfPlacements,
            int firstStudentId, int lastStudentId, BackendSession session) {
        threads = new Thread[lastStudentId - firstStudentId + 1];
        for (int i = 0; i < threads.length; ++i) {
            threads[i] = new Thread(new Student(firstListNumber, lastListNumber, listBaseName, numberOfPlacements,
                    firstStudentId + i, session));
        }
    }

    public void start(){
        for(Thread t:threads){
            t.start();
        }
    }
}
