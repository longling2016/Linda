import java.util.Comparator;

/**
 * Created by longlingwang on 4/14/17.
 */
public class Address implements Comparable<Address> {
    String hostName;
    String ip;
    int port;
    boolean ifAlive;
    public Address (String hostName, String ip, int port) {
        this.hostName = hostName;
        this.ip = ip;
        this.port = port;
        this.ifAlive = true;
    }
    public Address (String hostName, String ip, int port, boolean ifAlive) {
        this.hostName = hostName;
        this.ip = ip;
        this.port = port;
        this.ifAlive = ifAlive;
    }

    public int compareTo(Address a) {
        return hostName.compareTo(a.hostName);
    }


}
