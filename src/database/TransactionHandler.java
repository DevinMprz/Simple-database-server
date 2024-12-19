package database;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.*;

public class TransactionHandler implements Runnable{

    //komunikacia s pouzivatelom
    private final Socket socket;
    private PrintWriter writer;
    BufferedReader reader;
    private final Map<String, List<String>> localChanges;

    private final Map<String, List<String>> data;
    private final ArrayList<String> transaction = new ArrayList<>();

    //Mapy typu (command, handler)
    private final Map<String, Consumer<List<String>>> commandHandlers = new HashMap<>();
    private final Map<String, Runnable> specialCommands = new HashMap<>();

    private final ArrayList<String> synchronizedTabels = new ArrayList<>();

    public TransactionHandler(Socket socket, Map<String, List<String>> data) throws IOException {
        this.socket = socket;
        this.data = data;
        localChanges = new HashMap<>();

        commandHandlers.put("CREATE", this::handleCreateCommand);
        commandHandlers.put("SELECT", this::handleSelectCommand);
        commandHandlers.put("INSERT", this::handleInsertCommand);
        commandHandlers.put("DELETE", this::handleDeleteCommand);

        specialCommands.put("COMMIT", this::commitHandler);
        specialCommands.put("ABORT", this::abortHandler);
    }
    private void commitHandler(){
        transaction.forEach(this::handleCommand);

        //Zapisanie do globalnej databazy
        synchronized (data) {

            synchronizedTabels.forEach(table -> {
                data.put(table, localChanges.get(table));
            });

            localChanges.keySet().stream().filter(table -> !synchronizedTabels.contains(table))
                    .forEach(table -> data.get(table).addAll(localChanges.get(table)));
        }

        System.out.println("Commited.");
        writer.print("Commited. \n");
        writer.flush();
        transaction.clear();
        closeConnection();
    }
    private void abortHandler() {
        transaction.clear();
        writer.print("Aborted. \n");
        writer.flush();
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
        }catch (IllegalArgumentException e){
            writer.println("Error: " + e.getMessage() + ". Transaction aborted.");
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
        mergeTables(tableName);
        localChanges.get(tableName).addAll(List.of(parts.get(4).split(",")));
    }
    private void handleDeleteCommand(List<String> parts){
        String tableName = parts.get(2);
        mergeTables(tableName);
        try{
            localChanges.get(tableName).removeIf(word -> word.startsWith(parts.get(4)));
        }catch(IndexOutOfBoundsException e){
            localChanges.get(tableName).clear();
        }
    }

    //Vybral som si takyto sposob synchronizacie, lebo podla mna je kopirovat celu databazu do localChanges
    //moze byt casovo narocne pri velkych databazach, preto si beirem len potrebne tabulky.
    private void  mergeTables(String tableName){
        List<String> globalData = data.getOrDefault(tableName, new ArrayList<>());
        List<String> localData = localChanges.getOrDefault(tableName, new ArrayList<>());

        // Merge without duplicates
        localChanges.put(tableName, Stream.concat(globalData.stream(), localData.stream())
                                        .distinct()
                                        .collect(Collectors.toList()));
        synchronizedTabels.add(tableName);
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
