package cassdemo;

import java.io.InputStream;
import java.util.Scanner;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;

public class InputProcessor {
    private Scanner scanner;
    private BackendSession session;
    private boolean finish;

    public InputProcessor(InputStream inputStream, BackendSession session) {
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
                case "upsert":
                    executeUpsert(commandStrings);
                    return;
                case "delete":
                    executeDelete(commandStrings);
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
        System.out.println("help - displays this message");
        System.out.println("get lists - displays lists");
        System.out.println("get proposals - displays proposals");
        System.out.println("upsert list NAME MAX_SIZE - upserts a list named NAME with MAX_SIZE of places");
        System.out.println("exit - finishes execution of this program");
    }

    private void executeGet(String[] commandStrings) throws BackendException, ArrayIndexOutOfBoundsException {
        switch (commandStrings[1]) {
            case "lists":
                System.out.println(session.selectAllLists());
                return;
            case "proposals":
                System.out.println(session.selectAllProposals());
                return;
            default:
                System.out.println("Cannot get: " + commandStrings[1]);
                return;
        }
    }

    private void executeUpsert(String[] commandStrings) throws BackendException, ArrayIndexOutOfBoundsException {
        switch (commandStrings[1]) {
            case "list":
                session.upsertList(commandStrings[2], Integer.parseInt(commandStrings[3]));
                return;
            default:
                System.out.println("Cannot upsert: " + commandStrings[1]);
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

    private void executeExit() {
        finish = true;
        System.out.println("Exiting...");
    }
}
