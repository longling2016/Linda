import java.io.*;
import java.net.Socket;
import java.util.ArrayList;

/**
 * Created by longlingwang on 4/8/17.
 */
public class MessageSender {
    public void simpleSend (String message, String hostName, ArrayList<Address> addressBook) { // does NOT forward message when receiver is down

        int i = searchIndex(hostName, addressBook);

        if (!addressBook.get(i).ifAlive) {
            return;
        }

        try {
            Socket s = new Socket(addressBook.get(i).ip, addressBook.get(i).port);
            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            out.writeUTF(message);
            out.flush();
            out.close();
            s.close();
        } catch (IOException e) {
            addressBook.get(i).ifAlive = false;
            System.out.println("Host " + hostName + "is down for now.");
        }
    }

    public void directSend (String message, String ip, int port) {  // receiver is not in the address book yet
        try {

            Socket s = new Socket(ip, port);

            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            out.writeUTF(message);
            out.flush();
            out.close();
            s.close();

        } catch (IOException e) {
            System.out.println(e);
        }
    }

    public void send (String message, String sendToWho, ArrayList<Address> addressBook) { // forward message when receiver is down

        int i = searchIndex(sendToWho, addressBook);
        if (addressBook.get(i).ifAlive) { // receiver is alive
            message = "ori" + message;
        } else {
            message = "bup" + message;
            i = (i + 1) % addressBook.size();
        }

        try {
            Socket s = new Socket(addressBook.get(i).ip, addressBook.get(i).port);
            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            out.writeUTF(message);
            out.flush();
            out.close();
            s.close();

        } catch (IOException e) {
            addressBook.get(i).ifAlive = false;
            System.out.println("Host " + sendToWho + "is down for now. \nForward the message to its backup.");
            message = message.replace("ori", "bup");
            i = (i + 1) % addressBook.size();
            simpleSend(message, addressBook.get(i).hostName, addressBook);
        }
    }

    public int searchIndex(String hostName, ArrayList<Address> list) {
        for (int i = 0; i < list.size(); i ++) {
            if (list.get(i).hostName.equals(hostName)) {
                return i;
            }
        }
        return -1;
    }
}
