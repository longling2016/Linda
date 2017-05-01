import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Created by longlingwang on 4/19/17.
 */
public class ReArranger {
    public void reArrangeAdd(String filePath, Integer totalSlot, String localHost, Slot[] preTable,
                             Slot[] curTable, ArrayList<Address> preAddressBook, List<Address> curAddressBook) {

        int pre = preAddressBook.size();
        int cur = curAddressBook.size();
        MessageSender ms = new MessageSender();
        Search s = new Search();
        ArrayList<Address> difference = new ArrayList<>();

        for (Address a: curAddressBook) {
            boolean have = false;
            for (Address old: preAddressBook) {
                if (a.hostName.equals(old.hostName)) {
                    have = true;
                    break;
                }
            }
            if (!have) {
                difference.add(a);
            }
        }

        int givePerNew = (totalSlot / pre - totalSlot / cur) / difference.size();

        for (Address differ: difference) {

            HashMap<String, Integer> checkList = new HashMap<>();

            for (Address each: preAddressBook) {
                checkList.put(each.hostName, givePerNew);
            }

            for (Slot slot: curTable) {
                String curName = slot.hostBelong;
                if (!checkList.containsKey(curName) || checkList.get(curName) == 0) {
                    continue;
                }
                slot.hostBelong = differ.hostName;
                int n = checkList.get(curName) - 1;
                checkList.put(curName, n);
            }

        }

        HashMap<String, ArrayList<Integer>> map = new HashMap<>();

        for (int j = 0; j < preTable.length; j ++) {
            if (preTable[j].hostBelong.equals(localHost) && !curTable[j].hostBelong.equals(localHost) && preTable[j].tupleSaved) {

                String curNewName = curTable[j].hostBelong;
                if (map.containsKey(curNewName)) {
                    map.get(curNewName).add(j);
                } else {
                    ArrayList<Integer> list = new ArrayList<>();
                    list.add(j);
                    map.put(curNewName, list);
                }
            }
        }

        if (map.isEmpty()) {
            return;
        }

        for (String eachNew: map.keySet()) {

            HashSet<Integer> slotToTans = new HashSet<>(map.get(eachNew));

            // send the tuples in slotToTans to new host and the back up
            String tuplesToTans = getTuples(slotToTans, filePath + "tuples/original.txt");


            if (slotToTans.isEmpty()) {
                continue;
            }

            // send to original
            ms.simpleSend("orisav" + tuplesToTans, eachNew, curAddressBook);

            // delete the tuple from local
            s.removeTuple(tuplesToTans, filePath + "tuples/original.txt");

        }

    }


    public void reArrangeOB(String filePath, String localHost, Slot[] pre, Slot[] cur, List<Address> addressBook) {

        HashMap<String, ArrayList<Integer>> map = new HashMap<>();
        MessageSender ms = new MessageSender();
        Search s = new Search();

        for (int i = 0; i < pre.length; i ++) {
            if (pre[i].hostBelong.equals(localHost) && !cur[i].hostBelong.equals(localHost) && pre[i].tupleSaved) {

                String curNewName = cur[i].hostBelong;
                if (map.containsKey(curNewName)) {
                    map.get(curNewName).add(i);
                } else {
                    ArrayList<Integer> list = new ArrayList<>();
                    list.add(i);
                    map.put(curNewName, list);
                }
            }
        }

        if (map.isEmpty()) {
            return;
        }

        for (String eachNew: map.keySet()) {

            HashSet<Integer> slotToTans = new HashSet<>(map.get(eachNew));

            String tuplesToTans = getTuples(slotToTans, filePath + "tuples/original.txt");

            // send to original
            ms.simpleSend("orisav" + tuplesToTans, eachNew, addressBook);

            // delete the tuple from local
            s.removeTuple(tuplesToTans, filePath + "tuples/original.txt");

        }

    }


    public void reArrangeDelete (String filePath, String localHost, Slot[] slotTable, HashSet<String> deletedList, List<Address> addressBook) {
        // update address book
        MessageSender ms = new MessageSender();

        for (String cur: deletedList) {
            int i = ms.searchIndex(cur, addressBook);
            if (i != -1) {
                addressBook.remove(i);
            } else {
                System.out.println(cur + " is not one of the hosts in current distributed system.");
                return;
            }
        }

        ArrayList<HashSet<Integer>> sets = new ArrayList<>();
        int i = 0;
        int total = addressBook.size();

        for (Address eachCur: addressBook) {
           sets.add(new HashSet<>());
        }

        // update slot table and save the slot id for different hosts
        for (int index = 0; index < slotTable.length; index ++) {
            Slot cur = slotTable[index];
            if (cur.hostBelong.equals(localHost)) {
                if (cur.tupleSaved) {
                    sets.get(i).add(index);
                }
                i = (i + 1) % total;
            }
        }


        for (int each = 0; each < addressBook.size(); each ++) {

            Address receiver = addressBook.get(each);
            HashSet<Integer> slotToTans = sets.get(each);

            // send the tuples in deleted host to others and others' back up
            String tuplesToTans =  getTuples(slotToTans,filePath + "tuples/original.txt");

            if (!slotToTans.isEmpty()) {
                // send to original
                ms.simpleSend("orisav" + tuplesToTans, receiver.hostName, addressBook);
            }

            // send to backup
            Address curBackup = addressBook.get((each + 1) % addressBook.size());
            ms.simpleSend("bacsav" + receiver.hostName + "::" + tuplesToTans, curBackup.hostName, addressBook);
        }


        // forward its backup file to the new host
        StringBuilder sb = new StringBuilder();
        String backupWho = "";

        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath + "tuples/backup.txt"));
            String line;
            backupWho = br.readLine();
            while ((line = br.readLine()) != null) {
                sb.append("(" + line + ")");
            }
            br.close();
        } catch (IOException e) {
            System.out.println(e);
        }

        if (!deletedList.contains(backupWho)) {
            int newBackup = (ms.searchIndex(backupWho, addressBook) + 1) % addressBook.size();
            ms.simpleSend("bacsav" + backupWho + "::" + sb.toString(), addressBook.get(newBackup).hostName, addressBook);
        }

    }



    public String getTuples (HashSet<Integer> slotToTans, String filePath) {
        StringBuilder sb = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String line;
            while ((line = br.readLine()) != null) {
                String[] curline = line.split("->");
                if (curline.length < 2) {
                    continue;
                }
                int curSlot = Integer.parseInt(curline[0]);
                if (slotToTans.contains(curSlot)) {
                    sb.append("(" + line + ")");
                }
            }
            br.close();
            return sb.toString();
        } catch (IOException e) {
            System.out.println(e);
        }
        return null;
    }

}
