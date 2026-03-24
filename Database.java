import java.io.FileNotFoundException;
import java.io.IOException;

public class Database {
    private static StorageEngine st;
    Database(String FILE){
        
        st=new StorageEngine(FILE);
    }
    public void addAll() throws FileNotFoundException, IOException{
        st.putInd();
    }
    public void select(String key) throws Exception{
        st.get(key);
    }
    public void add(Record rec) throws IOException{
        st.put(rec);
    }
    public void select() throws FileNotFoundException, IOException {
        st.printAll();
    }
    public void delete(String key) throws FileNotFoundException, IOException {
        st.delete(key);
    }
}
