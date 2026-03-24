import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws Exception {
        Database db = new Database("database.db");
        Scanner sc = new Scanner(System.in);
        System.out.println("DATABASE STARTED");
        while (true) {
            String Method = sc.next();
            if (Method.equals("add")) {
                String key = sc.next();
                String value = sc.next();
                db.add(new Record(key, value));
            } else if (Method.equals("select_*")) {
                db.select();
            } else if (Method.equals("select")) {
                String key = sc.next();
                db.select(key);
            } else if (Method.equals("delete")) {
                String key = sc.next();
                db.select(key);
            } else if (Method.equals("end")) {
                break;
            } else {
                System.out.println("WRONG QUERY !!!");
            }
        }

    }
}
