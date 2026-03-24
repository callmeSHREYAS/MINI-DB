import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;

public class StorageEngine {
    private String FILE;
    private HashMap<String, Long> index = new HashMap<>();

    StorageEngine(String FILE) {
        this.FILE = FILE;
    }

    public void put(Record data) throws FileNotFoundException, IOException {
        String key = data.getKey();
        String value = data.getValue();
        try (RandomAccessFile raf = new RandomAccessFile(FILE, "rw")) {
            raf.seek(raf.length());
            long offset = raf.length();
            index.put(key, offset);
            raf.writeInt(key.length());
            raf.writeBytes(key);
            raf.writeInt(value.length());
            raf.writeBytes(value);
        }
    }

    public void printAll() throws FileNotFoundException, IOException {
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

                System.out.println(new String(keyBytes) + "->" + new String(valBytes));
            }
        }
    }

    public void putInd() throws FileNotFoundException, IOException {
        try (RandomAccessFile raf = new RandomAccessFile(FILE, "r")) {
            raf.seek(0);

            while (raf.getFilePointer() < raf.length()) {
                long cur = raf.getFilePointer();
                int keyLen = raf.readInt();
                byte[] keyBytes = new byte[keyLen];
                raf.readFully(keyBytes);

                int valLen = raf.readInt();
                byte[] valBytes = new byte[valLen];
                raf.readFully(valBytes);
                index.put(new String(keyBytes), cur);

            }
        }
    }

    public void get(String searchKey) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(FILE, "r")) {
            long st = index.getOrDefault(searchKey, -1L);
            if (st == -1) {
                System.out.println("Key Doesnt found");
            } else {
                raf.seek(st);
                int keyLen = raf.readInt();
                byte[] keyBytes = new byte[keyLen];
                raf.readFully(keyBytes);
                String key = new String(keyBytes);

                int valLen = raf.readInt();
                byte[] valBytes = new byte[valLen];
                raf.readFully(valBytes);
                String value = new String(valBytes);

                System.out.println(value);
            }

        }
        return;
    }

    public void delete(String key) throws IOException {
        if (index.getOrDefault(key, null) == null) {
            System.out.println("Key doesnt Exist");
            return;
        }
        try (RandomAccessFile raf = new RandomAccessFile(FILE, "r")) {
            raf.seek(0);

            while (raf.getFilePointer() < raf.length()) {
                raf.seek(raf.length());
                raf.writeInt(key.length());
                raf.writeBytes(key);
                raf.writeInt(0);
            }
            index.put(key,null); 
        }
    }
}
