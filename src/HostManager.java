import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.*;

/**
 * Created by longlingwang on 4/6/17.
 */
public class HostManager {

    public int checkNewHost (String hostList, String localIP, int localPort) {

        hostList = hostList.replaceAll(" ", "");

        // parse the list of hosts
        String[] hosts = hostList.split("\\)");

        for (String each : hosts) {
            String host = each.substring(1, each.length());
            String[] hostInfor = host.split(",");

            String hostName = hostInfor[0];
            String ip = hostInfor[1];
            String port = hostInfor[2];

            try {
                //try to connect to check the correctness of IP and port
                Socket s =new Socket();
                s.connect(new InetSocketAddress( ip, Integer.parseInt(port)),500);
                DataOutputStream out = new DataOutputStream(s.getOutputStream());
                out.writeUTF("confirm " + hostName + " " + localIP + " " + localPort);
                out.flush();
                out.close();
                s.close();
            } catch (IOException e) {
                System.out.println("Unreachable host for " + hostName + " at " + ip + ": " + port +"! Please enter again.");
                return -1;
            }

        }
        return hosts.length;
    }

    public boolean ifDuplicated (List<Address> list, HashSet<String> nameSet) {
        for (Address each: list) {
            if (nameSet.contains(each.hostName)) {
                return false;
            } else {
                nameSet.add(each.hostName);
            }
        }
        return true;
    }

    public void requireAddressBook (String[] hostList, String localIP, int localPort) {

        MessageSender ms = new MessageSender();

        for (String each : hostList) {
            String[] hostInfor = each.split(",");

            String ip = hostInfor[1];
            String port = hostInfor[2];
            ms.directSend("require " + localIP + " " + localPort, ip, Integer.parseInt(port));
        }
    }

    public void flushHostNet(List<Address> addressBook, String filePath, String localName) {
        File netFile = new File(filePath + "nets.txt");
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(netFile));
            for (Address each: addressBook) {
                if (!each.hostName.equals(localName)) {
                    out.write(each.hostName + ", " + each.ip + ", " + each.port + "\n");
                }
            }
            out.close();
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    public void initBackup(String filePath, String localName) {
        File netFile = new File(filePath + "tuples/backup.txt");
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(netFile));
            out.write(localName + "\n");
            out.close();
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    public void deletehosts (String filePath, String localHost, HashSet<String> deletedList, Slot[] slotTable, List<Address> addressBook) {
        // update address book
        MessageSender ms = new MessageSender();

        for (String cur: deletedList) {
            int i = ms.searchIndex(cur, addressBook);
            if (i != -1) {
                addressBook.remove(i);
            }
        }

        // update net file
        flushHostNet(addressBook, filePath, localHost);

        // update slot table
        HashMap<String, Integer> map = new HashMap<>();
        for (String each: deletedList) {
            map.put(each, 0);
        }
        for (Slot cur: slotTable) {
            String curHost = cur.hostBelong;
            if (deletedList.contains(curHost)) {
                int replace = map.get(curHost);
                map.put(curHost, (replace + 1) % addressBook.size());
                cur.hostBelong = addressBook.get(replace).hostName;
            }
        }
    }

}
