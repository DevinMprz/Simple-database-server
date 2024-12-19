package database;


import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

public class DatabaseServer implements Runnable{
    private final Map<String, List<String>> data = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private ServerSocket serverSocket;
    private int port = 8080;
    public void setPort(int port){
        this.port = port;
    }
    public void up(int port){
        //Start the server
        try(ServerSocket serverSocket = new ServerSocket(port)){
            System.out.println("Server is up.");
            this.serverSocket = serverSocket;
            while (!serverSocket.isClosed()) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("We have a connection here!");
                executor.submit(new TransactionHandler(clientSocket, data));
            }
        }catch (IOException e){
            System.out.println("Server is down.");
        } catch (NullPointerException e) {
            System.out.println("The end.");
        }
    }
    public void down(){
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
            executor.shutdown();
            System.out.println("Server has been shut down.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public Map<String, List<String>> getDataBase(){
        return data;
    }

    @Override
    public void run() {
        up(port);
    }
}
