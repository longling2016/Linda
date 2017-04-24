import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by longlingwang on 4/8/17.
 */
public class Broadcast {
    public void broadcast (String message, ArrayList<Address> addressBook) {

        for (int i = 0; i < addressBook.size(); i++) {
            Address cur = addressBook.get(i);
            if (!cur.ifAlive) {
                continue;
            }
            try {
                Socket s = new Socket(cur.ip, cur.port);
                DataOutputStream dOut = new DataOutputStream(s.getOutputStream());
                dOut.writeUTF(message);
                dOut.flush();
                dOut.close();
                s.close();
            } catch (IOException e) {
                System.out.println("Host " + cur.hostName + " is down for now!");
                cur.ifAlive = false;
            }
        }
    }

    public void rebootCast (String message, String filePath, String localHost) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath + "nets.txt"));
            String line = br.readLine();

            while ((line = br.readLine()) != null) {
                String[] curline = line.split("\\s*,\\s*");
                String ip = curline[1];
                int port = Integer.parseInt(curline[2]);
                try {
                    Socket s = new Socket(ip, port);
                    DataOutputStream out = new DataOutputStream(s.getOutputStream());
                    out.writeUTF(message);
                    out.flush();
                    out.close();
                    s.close();
                    break;
                } catch (IOException e) {
                    // current host may have been deleted. Continue trying next host.
                }
            }
                br.close();

        } catch (IOException e) {
            System.out.println(e);
        }
    }

}
