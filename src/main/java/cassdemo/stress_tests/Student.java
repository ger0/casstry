package cassdemo.stress_tests;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;

public class Student implements Runnable {
    private int firstListNumber;
    private int lastListNumber;
    private String listBaseName;
    private int id;
    BackendSession session;
    private List<Integer> placements;

    public Student(int firstListNumber, int lastListNumber, String listBaseName, int numberOfPlacements, int id,
            BackendSession session) {
        this.firstListNumber = firstListNumber;
        this.lastListNumber = lastListNumber;
        this.listBaseName = listBaseName;
        this.id = id;
        this.session = session;
        this.placements = generatePlacements(numberOfPlacements);
    }

    @Override
    public void run() {
        for(int i=firstListNumber;i<=lastListNumber;++i){
            try {
                session.upsertProposal(id, listBaseName+Integer.toString(i), placements);
            } catch (BackendException e) {
                //e.printStackTrace();
                session.increaseBackendExcepionCount();
            }
        }
    }

    private List<Integer> generatePlacements(int numberOfPlacements) {
        List<Integer> placements = new ArrayList<>(numberOfPlacements);
        for (int i = 1; i <= numberOfPlacements; ++i) {
            placements.add(i);
        }
        Collections.shuffle(placements);
        return placements;
    }
}
