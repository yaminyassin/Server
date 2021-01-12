import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;


public class JsonRepo {

    private final String filename;
    private JSONObject repositorio;

    public JsonRepo(String filename) {
        this.filename = filename;
        this.repositorio = (JSONObject)readFile(this.filename);
    }

    private Object readFile(String filename){
        JSONParser parser = new JSONParser();
        JSONObject data = new JSONObject();

        try {
           data = (JSONObject) parser.parse(new FileReader(filename));

        } catch (IOException | ParseException e) {
            System.err.println("Error on File Read");
        }
        return data;
    }

    private void printkeyExists(String key) {
        if (this.repositorio.get(key) == null)
            System.out.println("Key Doesn't Exist on Local");
        else
            System.out.println("Key Exists on Local");
    }


    public Object get(String key) {
        this.printkeyExists(key);

        return this.repositorio.get(key);
    }

    public void set(String key, String value){
        try {
            printkeyExists(key);
            this.repositorio.put(key, value);
            FileWriter writer = null;
            writer = new FileWriter(this.filename);
            writer.write(this.repositorio.toJSONString());
            writer.flush();
            writer.close();

        } catch (IOException e) {
           System.err.println("Error on File Write JsonRepo.set");
        }
    }

    public void rem(Object key){
        this.repositorio.remove(key);
        FileWriter writer = null;

        try {
            writer = new FileWriter(this.filename);
            writer.write(this.repositorio.toJSONString());

            writer.flush();
            writer.close();

        } catch (IOException e) {
            System.err.println("Error on File Write JsonRepo.rem");
        }
    }

    public boolean contains(String key){
        return repositorio.containsKey(key);
    }

    public void writeToFile(){
        try {
            FileWriter writer = null;
            writer = new FileWriter(this.filename);
            writer.write(this.repositorio.toJSONString());
            writer.flush();
            writer.close();

        } catch (IOException e) {
           System.err.println("Error on WriteToFile");
        }
    }

}





