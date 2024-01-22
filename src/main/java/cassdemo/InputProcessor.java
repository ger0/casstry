package cassdemo;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Scanner;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import cassdemo.stress_tests.ListsCreator;
import cassdemo.stress_tests.ProposalsCreator;

public class InputProcessor {
    private Scanner scanner;
    private BackendSession session;
    private Statistics statistics;
    private boolean finish;

    public InputProcessor(InputStream inputStream, BackendSession session, Statistics statistics) {
        this.statistics=statistics;
        scanner = new Scanner(inputStream);
        this.session = session;
        this.finish = false;
    }

    public void processInput() {
        String[] commandStrings;
        while (!finish) {
            commandStrings = getNextCommand();
            executeCommand(commandStrings);
        }
    }

    public String[] getNextCommand() {
        StringBuilder sb = new StringBuilder();
        String line = scanner.nextLine();
        while (line.substring(line.length() - 1).equals("\\")) {
            sb.append(line);
            sb.append(" ");
            line = scanner.nextLine();
        }
        sb.append(line);// last line
        return sb.toString().split(" ");
    }

    public void executeCommand(String[] commandStrings) {
        try {
            switch (commandStrings[0]) {
                case "help":
                    executeHelp();
                    return;
                case "get":
                    executeGet(commandStrings);
                    return;
                case "post":
                    executePost(commandStrings);
                    return;
                case "delete":
                    executeDelete(commandStrings);
                    return;
                case "reapply":
                    reapply(commandStrings);
                    return;
                case "stress":
                    stressTests(commandStrings);
                    return;
                case "exit":
                    executeExit();
                    return;
                default:
                    System.out.println("Unrecognized command");
                    return;
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            System.out.println("Wrong arguments");
            return;
        } catch (BackendException e) {
            System.out.println("Got backend exception: " + e.getMessage());
            return;
        }
    }

    private void executeHelp() {
        System.out.println("Program allowing to create of ordered lists with specyfing of placement preferences");
        System.out.println("Usage:");
        System.out.println("help - displays this message");
        System.out.println("get lists - displays lists");
        System.out.println("get proposals - displays proposals");
        System.out.println("get statistics - displays statistics");
        System.out.println("post list NAME MAX_SIZE - inserts a list named NAME with MAX_SIZE of places");
        System.out.println(
                "post proposal STUDENT_ID LIST_NAME PLACEMENT_1 PLACEMENT_2 ... - proposes PLACEMENTS for sepcifeid student in specified list");
        System.out.println("\texample: post proposal 123456 seminarium 1 5 9 2 6 10 3 7 11 4 8 12");
        System.out.println("reapply STUDENT_ID LIST_NAME - reapplies student's proposal into specified list");
        System.out.println(
                "stress lists BASE_NAME FIRST_NUMBER LAST_NUMBER SIZE_OF_LIST - creates lists with specified name and range of suffixes");
        System.out.println("\texample: stress lists test 1 5 40");
        System.out.println(
                "stress proposals LIST_BASE_NAME FIRST_LIST_NUMBER LAST_LIST_NUMBER LIST_SIZE FIRST_STUDENT_ID LAST_STUDENT_ID");
        System.out
                .println("\t- creates proposals for specifeid range of lists by students with specified range of ids");
        System.out.println("\texample: stress proposals test 1 5 40 101 110");
        System.out.println("exit - finishes execution of this program");
        System.out.println();
    }

    private void executeGet(String[] commandStrings) throws BackendException, ArrayIndexOutOfBoundsException {
        switch (commandStrings[1]) {
            case "lists":
                System.out.println(session.selectAllLists());
                return;
            case "proposals":
                System.out.println(session.selectAllProposals());
                return;
            case "statistics":
                System.out.println(statistics.toString());
                return;
            default:
                System.out.println("Cannot get: " + commandStrings[1]);
                return;
        }
    }

    private void reapply(String[] commandStrings) throws BackendException, ArrayIndexOutOfBoundsException {
        session.reapplyProposal(Integer.parseInt(commandStrings[1]), commandStrings[2]);
    }

    private void executePost(String[] commandStrings) throws BackendException, ArrayIndexOutOfBoundsException {
        switch (commandStrings[1]) {
            case "list":
                session.insertList(commandStrings[2], Integer.parseInt(commandStrings[3]));
                return;
            case "proposal":
                ArrayList<Integer> placements = new ArrayList<Integer>(commandStrings.length - 4);
                for (int i = 4; i < commandStrings.length; ++i) {
                    placements.add(Integer.parseInt(commandStrings[i]));
                }
                session.insertProposal(Integer.parseInt(commandStrings[2]), commandStrings[3], placements);
                return;
            default:
                System.out.println("Cannot post: " + commandStrings[1]);
                return;
        }
    }

    private void executeDelete(String[] commandStrings) throws BackendException, ArrayIndexOutOfBoundsException {
        switch (commandStrings[1]) {
            case "lists":
                session.deleteAllLists();
                return;
            default:
                System.out.println("Cannot delete: " + commandStrings[1]);
                return;
        }
    }

    private void stressTests(String[] commandStrings) throws BackendException, ArrayIndexOutOfBoundsException {
        switch (commandStrings[1]) {
            case "lists":
                ListsCreator lc = new ListsCreator(commandStrings[2], Integer.parseInt(commandStrings[3]),
                        Integer.parseInt(commandStrings[4]), Integer.parseInt(commandStrings[5]), session);
                lc.create();
                return;
            case "proposals":
                ProposalsCreator pc = new ProposalsCreator(commandStrings[2], Integer.parseInt(commandStrings[3]),
                        Integer.parseInt(commandStrings[4]), Integer.parseInt(commandStrings[5]),
                        Integer.parseInt(commandStrings[6]), Integer.parseInt(commandStrings[7]), session);
                pc.start();
                return;
            default:
                System.out.println("Unknown stress argument " + commandStrings[1]);
                return;
        }
    }

    private void executeExit() {
        finish = true;
        System.out.println("Exiting...");
    }
}
