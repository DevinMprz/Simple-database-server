import database.DatabaseServer;

public class Main {
    public static void main(String[] args) {
        DatabaseServer db = new DatabaseServer();
        db.up(8080);
        db.up(8080);
    }
}