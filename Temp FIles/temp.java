import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Scanner;

public class temp {
    static final String FILE = "db.bin";

    // ✅ WRITE a key-value pair
    public static void put(String key, String value) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(FILE, "rw")) {
            raf.seek(raf.length()); // go to END of file (append)

            raf.writeInt(key.length()); // [key_length]
            raf.writeBytes(key); // [key]
            raf.writeInt(value.length()); // [value_length]
            raf.writeBytes(value); // [value]
        }
    }

    // ✅ READ a value by key
    public static String get(String searchKey) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(FILE, "r")) {
            raf.seek(0); 
            while (raf.getFilePointer() < raf.length()) {
                int keyLen = raf.readInt(); // read key_length
                byte[] keyBytes = new byte[keyLen];
                raf.readFully(keyBytes); // read key
                String key = new String(keyBytes);

                int valLen = raf.readInt(); // read value_length
                byte[] valBytes = new byte[valLen];
                raf.readFully(valBytes); // read value
                String value = new String(valBytes);

                if (key.equals(searchKey)) {
                    return value; // 🎯 found it!
                }
            }
        }
        return null; // not found
    }

    // ✅ PRINT all key-value pairs
    public static void printAll() throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(FILE, "r")) {
            raf.seek(0);
            System.out.println("=== DATABASE ===");
            while (raf.getFilePointer() < raf.length()) {
                int keyLen = raf.readInt();
                byte[] keyBytes = new byte[keyLen];
                raf.readFully(keyBytes);

                int valLen = raf.readInt();
                byte[] valBytes = new byte[valLen];
                raf.readFully(valBytes);

                System.out.println(new String(keyBytes) + " → " + new String(valBytes));
            }
        }
    }

    public static void main(String[] args) throws IOException {

    }
}