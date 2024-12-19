/*
 *
 *
 *  ---BONUSOVE ULOHY---
 *  Spravil som to na VSETKY bonusove. Dufam teda ze stream-ove cykly nie je zakazane pouzivat.
 * No teda a "if podobne" volania typu map.computeIfAbsent apod.
 *
 * Tu validaciu som spravil tak, ze ak sa chyti nejaky exception, tak sa abortne tranzakcia, teda je to pre rozne
 * metody rozne ale koncept rovnaky
 *
 * ---EXECUTOR---
 * Kedze je poziadavka na paralelny pristup, tak by som mal pouzit threads na to.
 * Pouzil som si executor, konkretne newCachedThreadPool.
 * Tento typ executoru je vhodny pre tuto situaciu, kedze sa vytvara mnoho krat novy thread a zase zanika.
 * Dufam ze toto je v pohode, a nemusim puzivat prave Thread-y.
 * Este k tomu, TransactionHandler implementuje Runnable, cize je navrhnuty na spustenie vo vlastnom vlakne.
 *
 *
 *  ---INE---
 * Mohli ste si vsimnut ze tu su gitignore subory apod.
 * Zaujmaju ma taketo projekty, tak som vytvoril git repo pre toto ak rozhodnem to vylepsit a nieco pridat.
 * Keby ste chceli pozriet, tak je to tu: https://github.com/DevinMprz/Simple-database-server
 */

package database;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
public class DatabaseServer{
    private final Map<String, List<String>> data = new ConcurrentHashMap<>();

    //preco som pouzil executor je popisane hore.
    private final ExecutorService executor = Executors.newCachedThreadPool();

    private ServerSocket serverSocket;
    private Thread serverThread;

    public void up(int port){
         serverThread = new Thread(() -> {
            try(ServerSocket serverSocket = new ServerSocket(port)){

                System.out.println("Server je zapnuty.");
                this.serverSocket = serverSocket;

                while (!serverSocket.isClosed()) {
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("Nove pripojenie!");
                    executor.submit(new TransactionHandler(clientSocket, data));
                }
            }catch (Exception e){
                System.out.println("Error while starting server: " + e.getMessage());
                e.printStackTrace();
            }
        });
        serverThread.start();
    }
    public void down(){
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
            if (serverThread != null && serverThread.isAlive()) {
                serverThread.interrupt();
            }
            executor.shutdown();
            System.out.println("Server bol vypnuty.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
