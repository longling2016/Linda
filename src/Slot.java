/**
 * Created by longlingwang on 4/19/17.
 */
public class Slot {
    boolean tupleSaved;
    String hostBelong;
    public Slot(String hostName) {
        this.hostBelong = hostName;
        this.tupleSaved = false;
    }
    public Slot(String hostName, boolean tupleSaved ) {
        this.hostBelong = hostName;
        this.tupleSaved = tupleSaved;
    }
}
