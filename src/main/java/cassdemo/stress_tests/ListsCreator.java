package cassdemo.stress_tests;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;

public class ListsCreator {
    private int firstNumber;
    private int lastNumber;
    private int sizeOfList;
    private BackendSession session;
    private String baseName;

    public ListsCreator(String baseName, int firstNumber, int lastNumber, int sizeOfList, BackendSession session) {
        this.firstNumber = firstNumber;
        this.lastNumber = lastNumber;
        this.sizeOfList = sizeOfList;
        this.session = session;
        this.baseName = baseName;
    }

    public void create() throws BackendException {
        for (int i = firstNumber; i <= lastNumber; ++i) {
            session.insertList(baseName+Integer.toString(i), sizeOfList);
        }
    }
}
