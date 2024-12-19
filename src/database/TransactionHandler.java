package database;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.*;

public class TransactionHandler implements Runnable{

    //komunikacia s pouzivatelom
    private final Socket socket;
    private PrintWriter writer;
    BufferedReader reader;
    private final Map<String, List<String>> localChanges;

    private final Map<String, ReentrantLock> tableLocks;

    private final Map<String, List<String>> data;
    private final ArrayList<String> transaction = new ArrayList<>();

    //Mapy typu (command, handler)
    private final Map<String, Consumer<List<String>>> commandHandlers = new HashMap<>();
    private final Map<String, Runnable> specialCommands = new HashMap<>();

    public TransactionHandler(Socket socket, Map<String, List<String>> data) throws IOException {
        this.socket = socket;
        this.data = data;
        localChanges = new HashMap<>();
        this.tableLocks = new ConcurrentHashMap<>();

        commandHandlers.put("CREATE", this::handleCreateCommand);
        commandHandlers.put("SELECT", this::handleSelectCommand);
        commandHandlers.put("INSERT", this::handleInsertCommand);
        commandHandlers.put("DELETE", this::handleDeleteCommand);

        specialCommands.put("COMMIT", this::commitHandler);
        specialCommands.put("ABORT", this::abortHandler);
    }
    private void commitHandler(){
        try {
            transaction.forEach(this::handleCommand);

            synchronized (data) {
                List<String> tablesToLock = new ArrayList<>(localChanges.keySet());
                lockTablesInOrder(tablesToLock);

                try {
                    localChanges.forEach((table, changes) -> data.put(table, new ArrayList<>(changes)));
                } finally {
                    unlockTablesInOrder(tablesToLock);
                }
            }

            writer.println("Commited.");
            writer.flush();
            transaction.clear();
        } catch (Exception e) {
            writer.println("Error during commit: " + e.getMessage() + ". Transaction aborted.");
            e.printStackTrace();
            abortHandler();
        } finally {
            closeConnection();
        }
    }
    private void abortHandler() {
        transaction.clear();
        writer.print("Aborted. \n");
        writer.flush();
        unlockTablesInOrder(tableLocks.keySet().stream().toList());
        closeConnection();
    }

    private void closeConnection(){
        try {
            writer.close();
            reader.close();
            socket.close();
        } catch (IOException e) {
            writer.println("Error while closing the connection: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // metody pre handlovanie prikazov
    private void handleCommand(String line){
        List<String> parts = List.of(line.split(" "));
        try{
            commandHandlers
                    .getOrDefault(parts.getFirst(), (x) -> {throw new IllegalArgumentException("Unknown command: " + x.getFirst());})
                    .accept(parts);
        }catch (Exception e) {
            writer.println("Error: " + e.getMessage() + ". Transaction aborted.");
            e.printStackTrace();
            abortHandler();
        }
    }
    private void handleCreateCommand(List<String> parts){
        String tableName = parts.get(2);
        localChanges.put(tableName, new ArrayList<>());
    }
    private void handleSelectCommand(List<String> parts){
        String tableName = parts.get(2);

        String filterCriteria = parts.stream()
                .skip(4)
                .findFirst()
                .orElse("");
        //Viem, ze by sa toto dalo aj tak, ale nie som si isty, ze to sa nepovažuje za "if"
        // String filterCriteria = parts.size() > 4 ? parts.get(4) : "";

        mergeTables(tableName);

        System.out.println("Merged table: " + tableName + " " + localChanges.get(tableName));
        writer.print("Words from table " + tableName + " that starts with " + filterCriteria + ": ");
            writer.println(
                    localChanges.get(tableName).stream()
                            .filter(word -> word.startsWith(filterCriteria))
                            .collect(Collectors.joining(", ")));
            writer.flush();
    }
    private void handleInsertCommand(List<String> parts){
        String tableName = parts.get(2);

        acquireTableLock(tableName);
        try {
            mergeTables(tableName);
            localChanges.get(tableName).addAll(List.of(parts.get(4).split(",")));
        } finally {
            releaseTableLock(tableName);
        }
    }
    private void handleDeleteCommand(List<String> parts){
        String tableName = parts.get(2);

        acquireTableLock(tableName);
        try{
            mergeTables(tableName);
            localChanges.get(tableName).removeIf(word -> word.startsWith(parts.get(4)));
        }catch(IndexOutOfBoundsException e){
            localChanges.get(tableName).clear();
        } catch (Exception e) {
            throw new RuntimeException("Error while deleting: " + e.getMessage(), e);
        }
        finally {
            releaseTableLock(tableName);
        }
    }

    //metody na pracu s zamkami
    private void acquireTableLock(String tableName) {
        tableLocks.computeIfAbsent(tableName, _ -> new ReentrantLock()).lock();
    }
    private void releaseTableLock(String tableName) {
        tableLocks.computeIfAbsent(tableName, _ -> new ReentrantLock()).unlock();
    }
    private void lockTablesInOrder(List<String> tables){
        tables.stream().sorted().forEach(this::acquireTableLock);
    }
    private void unlockTablesInOrder(List<String> tables){
        tables.stream().sorted().forEach(this::releaseTableLock);
    }

    //Vybral som si takyto sposob synchronizacie, lebo podla mna je kopirovat celu databazu do localChanges
    //moze byt casovo narocne pri velkych databazach, preto si beirem len potrebne tabulky.
    private void mergeTables(String tableName) {
        List<String> globalData = data.getOrDefault(tableName, new ArrayList<>());
        List<String> localData = localChanges.getOrDefault(tableName, new ArrayList<>());

        localChanges.put(tableName, Stream.concat(globalData.stream(), localData.stream())
                .distinct()
                .collect(Collectors.toList()));
    }

    @Override
    public void run() {
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter writer = new PrintWriter(socket.getOutputStream())){
            this.writer = writer;
            this.reader = reader;
            writer.println("Welcome to the database server. \n");
            writer.flush();
            reader.lines().forEach(line -> {
                System.out.println("Processing: " + line);
                specialCommands.getOrDefault(line, () -> transaction.add(line)).run();
            });

        }catch (IOException e) {
            writer.println("Error while reading commands: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
