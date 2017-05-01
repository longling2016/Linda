import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

/**
 * Created by longlingwang on 4/8/17.
 */
public class Broadcast {
    public void broadcast (String message, List<Address> addressBook, String localHost) {

        for (int i = 0; i < addressBook.size(); i++) {
            Address cur = addressBook.get(i);
            if (!cur.ifAlive || cur.hostName.equals(localHost)) {
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

    public void broadcastForward (String message, List<Address> addressBook, String localHost) {
        MessageSender ms = new MessageSender();

        for (int i = 0; i < addressBook.size(); i++) {
            Address cur = addressBook.get(i);
            if (cur.hostName.equals(localHost)) {
                continue;
            }

            ms.send(message, cur.hostName, addressBook);
        }
    }

    public boolean rebootCast (String message, String filePath) {
        boolean ifSent = false;
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath + "nets.txt"));
            String line;
            while ((line = br.readLine()) != null) {

                String[] curline = line.split("\\s*,\\s*");
                String ip = curline[1];
                int port = Integer.parseInt(curline[2]);
                try {

                    Socket s =new Socket();
                    s.connect(new InetSocketAddress( ip, port),500);
                    DataOutputStream out = new DataOutputStream(s.getOutputStream());
                    out.writeUTF(message);
                    out.flush();
                    out.close();
                    s.close();
                    ifSent = true;
                    break;
                } catch (IOException e) {
                    // current host may have been deleted. Continue trying next host.
                }
            }
            br.close();

        } catch (IOException e) {
            System.out.println(e);
        }
        return ifSent;
    }

}
