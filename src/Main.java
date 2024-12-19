import database.DatabaseServer;

public class Main {
    public static void main(String[] args) {
        DatabaseServer db = new DatabaseServer();
        Thread serverThread = new Thread(db);
        db.setPort(33345);
        serverThread.start();
        for (int i = 0; i < 999; i++) {
            System.out.println(db.getDataBase());
            try {
                Thread.sleep(3000); // Simulate work
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        db.down();
    }
}