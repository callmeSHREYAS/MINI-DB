import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

public class StorageEngine {
    private String FILE;
    private HashMap<String, Long> index = new HashMap<>();

    StorageEngine(String FILE) {
        this.FILE = FILE;
    }

    public void putInd() throws FileNotFoundException, IOException {
        try (RandomAccessFile raf = new RandomAccessFile(FILE, "rw")) {
            raf.seek(0);

            while (raf.getFilePointer() < raf.length()) {
                long cur = raf.getFilePointer();
                int keyLen = raf.readInt();
                byte[] keyBytes = new byte[keyLen];
                raf.readFully(keyBytes);

                int valLen = raf.readInt();
                byte[] valBytes = new byte[valLen];

                if (valLen == 0) {
                    index.remove(new String(keyBytes));
                } else {
                    raf.readFully(valBytes);
                    index.put(new String(keyBytes), cur);
                }
            }
        }
        System.out.println(index);
    }

    public void put(Record data) throws FileNotFoundException, IOException {
        String key = data.getKey();
        String value = data.getValue();
        try (RandomAccessFile raf = new RandomAccessFile(FILE, "rw")) {
            long offset = raf.length();
            raf.seek(offset);
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

            for (Map.Entry<String, Long> entry : index.entrySet()) {
                String key = entry.getKey();
                long val = entry.getValue();
                long st = val;
                raf.seek(st);
                int keyLen = raf.readInt();
                byte[] keyBytes = new byte[keyLen];
                raf.readFully(keyBytes);
                String t_key = new String(keyBytes);

                int valLen = raf.readInt();
                if (valLen == 0) {
                    index.remove(new String(keyBytes));
                    continue;
                }
                byte[] valBytes = new byte[valLen];
                raf.readFully(valBytes);
                String value = new String(valBytes);

                System.out.println(key + " -> " + value);
            }

        }
    }

    public void get(String searchKey) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(FILE, "r")) {
            long st = index.getOrDefault(searchKey, -1L);
            if (st == -1) {
                System.out.println("Key Not found");
            } else {
                raf.seek(st);
                int keyLen = raf.readInt();
                byte[] keyBytes = new byte[keyLen];
                raf.readFully(keyBytes);
                String key = new String(keyBytes);

                int valLen = raf.readInt();
                if (valLen == 0) {
                    // index.remove(new String(key));
                    System.out.println("Key Not Found");
                    return;
                }
                byte[] valBytes = new byte[valLen];
                raf.readFully(valBytes);
                String value = new String(valBytes);

                System.out.println(value);
            }

        }
        return;
    }

    public void delete(String key) throws IOException {

        try (RandomAccessFile raf = new RandomAccessFile(FILE, "rw")) {
            long st = index.getOrDefault(key, -1L);
            if (st == -1) {
                System.out.println("Key Not Found");
                return;
            }
            raf.seek(st);
            int keyLen = raf.readInt();
            byte[] keyBytes = new byte[keyLen];
            raf.readFully(keyBytes);
            String temp_key = new String(keyBytes);

            int valLen = raf.readInt();
            byte[] valBytes = new byte[valLen];
            raf.readFully(valBytes);
            String value = new String(valBytes);
            // System.out.println(valLen); will 0 if value is null
            if (valLen == 0) {
                index.remove(new String(keyBytes));
                System.out.println("Key Not Found");
                return;
            } else {
                index.remove(new String(keyBytes));
                raf.seek(raf.length());
                raf.writeInt(key.length());
                raf.writeBytes(key);
                raf.writeInt(0);
            }

        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public void compact() throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(FILE, "rw")) {
            raf.seek(0);
            RandomAccessFile rf = new RandomAccessFile("temp.db", "rw");
            rf.seek(0);

            HashMap<String, Long> temp_ind = new HashMap<>();

            for (Map.Entry<String, Long> entry : index.entrySet()) {
                String key = entry.getKey();
                long val = entry.getValue();
                long st = val;
                raf.seek(st);
                int keyLen = raf.readInt();
                byte[] keyBytes = new byte[keyLen];
                raf.readFully(keyBytes);
                String t_key = new String(keyBytes);
                int valLen = raf.readInt();
                if (valLen == 0) {
                    index.remove(new String(keyBytes));
                    continue;
                }
                byte[] valBytes = new byte[valLen];
                raf.readFully(valBytes);
                String value = new String(valBytes);
                long offset = rf.length();
                rf.seek(offset);
                temp_ind.put(t_key, offset);
                rf.writeInt(t_key.length());
                rf.writeBytes(key);
                rf.writeInt(value.length());
                rf.writeBytes(value);
            }
            index.clear();
            index.putAll(temp_ind);
            File fileToDelete = new File(FILE);
            fileToDelete.delete(); 
            // File oldFile = new File("temp.db");
            // oldFile.renameTo(fileToDelete);
        }
    }
}
