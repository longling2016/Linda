import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Created by longlingwang on 4/19/17.
 */
public class ReArranger {
    public void reArrangeAdd(String filePath, Integer totalSlot, String localHost,
                             Slot[] slotTable, ArrayList<Address> preAddressBook, List<Address> curAddressBook) {
        int pre = preAddressBook.size();
        int cur = curAddressBook.size();
        MessageSender ms = new MessageSender();
        Search s = new Search();
        ArrayList<Address> difference = new ArrayList<>();
        for (Address a: curAddressBook) {
            for (Address old: preAddressBook) {
                if (a.equals(old)) {
                    break;
                }
            }
            difference.add(a);
        }

        int givePerNew = (totalSlot / pre - totalSlot / cur) / difference.size();

        for (Address newAdd: difference) {
            HashSet<Integer> slotToTans = new HashSet<>();
            HashMap<String, Integer> counter = new HashMap<>();
            for (Address each: preAddressBook) {
                counter.put(each.hostName, givePerNew);
            }
            int i = 0;
            while (!ifEmpty(counter)) {
                String curName = slotTable[i].hostBelong;
                if (!counter.containsKey(curName)) {
                    i ++;
                    continue;
                }
                int n = counter.get(curName);
                if (n > 0) {
                    slotTable[i].hostBelong = newAdd.hostName;
                    n --;
                    counter.put(curName, n);
                }
                if (curName.equals(localHost) && slotTable[i].tupleSaved) {
                    slotToTans.add(i);
                }
                i ++;
            }

            if (slotToTans.isEmpty()) {
                continue;
            }

            // send the tuples in slotToTans to new host and the back up
            String tuplesToTans =  getTuples(slotToTans,filePath + "tuples/original.txt");

            // if new host is alive (it should be alive), send to original
            ms.directSend("orisav" + tuplesToTans, newAdd.ip, newAdd.port);

            // if backup of new added is on, send backup
            Address curNewBackup = curAddressBook.get((ms.searchIndex(newAdd.hostName, curAddressBook) + 1) % cur);
            if (curNewBackup.ifAlive) {
                ms.simpleSend("bacsav" + newAdd.hostName + "::" + tuplesToTans, curNewBackup.hostName, curAddressBook);
            }

            // delete the tuple from local
            s.removeTuple(tuplesToTans,filePath + "tuples/original.txt");

            //delete the tuple from backup
            Address backup = curAddressBook.get((ms.searchIndex(localHost, curAddressBook) + 1) % cur);
            if (backup.ifAlive) {
                ms.simpleSend("bacdel " + tuplesToTans, backup.hostName, curAddressBook);
            }

        }

    }

    public void reArrangeRea(String filePath, Integer totalSlot, String localHost,
                             Slot[] slotTable, ArrayList<Address> preAddressBook, List<Address> curAddressBook) {
        // get the backup host name

        String backupName = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath + "tuples/backup.txt"));
            backupName = br.readLine();
            br.close();
        } catch (IOException e) {
            System.out.println(e);
        }

        if (backupName.equals("")) { // backup file is empty
            return;
        }

        int pre = preAddressBook.size();
        int cur = curAddressBook.size();
        MessageSender ms = new MessageSender();
        Search s = new Search();
        ArrayList<Address> difference = new ArrayList<>();
        for (Address a: curAddressBook) {
            for (Address old: preAddressBook) {
                if (a.equals(old)) {
                    break;
                }
            }
            difference.add(a);
        }

        int givePerNew = (totalSlot / pre - totalSlot / cur) / difference.size();

        for (Address newAdd: difference) {
            HashSet<Integer> slotToTans = new HashSet<>();
            HashSet<Integer> backupToTans = new HashSet<>();
            HashMap<String, Integer> counter = new HashMap<>();
            for (Address each: preAddressBook) {
                counter.put(each.hostName, givePerNew);
            }
            int i = 0;
            while (!ifEmpty(counter)) {
                String curName = slotTable[i].hostBelong;
                int n = counter.get(curName);
                if (n > 0) {
                    slotTable[i].hostBelong = newAdd.hostName;
                    n --;
                    counter.put(curName, n);
                }
                if (curName.equals(localHost) && slotTable[i].tupleSaved) {
                    slotToTans.add(i);
                }

                if (curName.equals(backupName) && slotTable[i].tupleSaved) {
                    backupToTans.add(i);
                }
                i ++;
            }

            // send the tuples in slotToTans to new host and the back up
            String tuplesToTans =  getTuples(slotToTans,filePath + "tuples/original.txt");

            // send the tuples in backupToTans to new host
            String backupTuplesToTans =  getTuples(slotToTans,filePath + "tuples/backup.txt");

            // if new host is alive (it should be alive), send to original
            ms.directSend("orisav" + tuplesToTans, newAdd.ip, newAdd.port);
            ms.directSend("orisav" + backupTuplesToTans, newAdd.ip, newAdd.port);

            // if backup of new added is on, send backup
            Address curNewBackup = curAddressBook.get((ms.searchIndex(newAdd.hostName, curAddressBook) + 1) % cur);
            ms.simpleSend("bacsav" + newAdd.hostName + "::" + tuplesToTans, curNewBackup.hostName, curAddressBook);
            ms.simpleSend("bacsav" + newAdd.hostName + "::" + backupTuplesToTans, curNewBackup.hostName, curAddressBook);

            // delete the tuple from local
            s.removeTuple(tuplesToTans,filePath + "tuples/original.txt");

            // delete the tuple from backup
            s.removeTuple(backupTuplesToTans,filePath + "tuples/backup.txt");

            //delete the tuple from backup host of local host
            Address backup = curAddressBook.get((ms.searchIndex(localHost, curAddressBook) + 1) % cur);
            ms.simpleSend("bacdel " + tuplesToTans, backup.hostName, curAddressBook);
        }

    }

    public void reArrangeDelete (String filePath, String localHost, Slot[] slotTable, HashSet<String> deletedList, List<Address> addressBook) {
        // update address book
        for (Address cur: addressBook) {
            if (deletedList.contains(cur.hostName)) {
                addressBook.remove(cur);
            }
        }

        ArrayList<HashSet<Integer>> sets = new ArrayList<>(addressBook.size());
        int i = 0;
        int total = addressBook.size();

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

        MessageSender ms = new MessageSender();

        for (int each = 0; each < addressBook.size(); each ++) {

            Address receiver = addressBook.get(each);
            HashSet<Integer> slotToTans = sets.get(each);

            // send the tuples in deleted host to others and others' back up
            String tuplesToTans =  getTuples(slotToTans,filePath + "tuples/original.txt");

            // send to original
            ms.simpleSend("orisav" + tuplesToTans, receiver.hostName, addressBook);

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

    public boolean ifEmpty(HashMap<String, Integer> hm) {
        for (Integer each: hm.values()) {
            if (each > 0) {
                return false;
            }
        }
        return true;
    }


}
