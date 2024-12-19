package database;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class TransactionHandler implements Runnable{

    //komunikacia s pouzivatelom
    private final Socket socket;
    private PrintWriter writer;
    BufferedReader reader;
    private Map<String, List<String>> localChanges;

    private final Map<String, List<String>> data;
    private final ArrayList<String> transaction = new ArrayList<>();

    //Mapy typu (command, handler)
    private final Map<String, Consumer<List<String>>> commandHandlers = new HashMap<>();
    private final Map<String, Runnable> specialCommands = new HashMap<>();

    public TransactionHandler(Socket socket, Map<String, List<String>> data) throws IOException {
        this.socket = socket;
        this.data = data;
        localChanges = new HashMap<>(data);

        commandHandlers.put("CREATE", this::handleCreateCommand);
        commandHandlers.put("SELECT", this::handleSelectCommand);
        commandHandlers.put("INSERT", this::handleInsertCommand);
        commandHandlers.put("DELETE", this::handleDeleteCommand);

        specialCommands.put("COMMIT", this::commitHandler);
        specialCommands.put("ABORT", this::abortHandler);
    }
    private void commitHandler(){
        transaction.forEach(this::handleCommand);
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
        data.put(tableName, new ArrayList<>());
    }
    private void handleSelectCommand(List<String> parts){
        String tableName = parts.get(2);
        String filterCriteria = parts.stream()
                .skip(4)
                .findFirst()
                .orElse("");
        writer.print("Words from table " + tableName + " that starts with " + filterCriteria + ": ");
            writer.println(
                    data.get(tableName).stream()
                            .filter(word -> word.startsWith(filterCriteria))
                            .collect(Collectors.joining(", ")));
            writer.flush();
    }
    private void handleInsertCommand(List<String> parts){
        String tableName = parts.get(2);
        data.get(tableName).addAll(List.of(parts.get(4).split(",")));
    }
    private void handleDeleteCommand(List<String> parts){
        String tableName = parts.get(2);
        try{
            data.get(tableName).removeIf(word -> word.startsWith(parts.get(4)));
        }catch(IndexOutOfBoundsException e){
            data.get(tableName).clear();
        }
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
