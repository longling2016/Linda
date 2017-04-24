import com.sun.tools.javah.Util;

import java.io.File;
import java.io.IOException;
import java.net.*;
import java.util.*;

public class P2 {
    static ServerSocket serverSocket;
    static String ip;
    static int port;
    static String hostName;
    static String filePath;
    static ArrayList<Address> addressBook;
    static ArrayList<Address> oldBackupAddressBook;
    static Boolean lock = false;
    static String aws;
    static String awsName;
    static int needACK;
    static String errorMessage;
    static Slot[] slotTable = new Slot[65536];
    static Integer totalSlot = 65536;


    public static void main (String[] args) {

        if (args.length != 1) {
            System.out.println("Please execute the program with one word host name such that 'host_1'...");
            return;
        }

        //get the host name from user input
        hostName = args[0];

        // get the IP and available port
        try {
            ip = InetAddress.getLocalHost().getHostAddress();

            // specify a port of 0 to the ServerSocket constructor and it will listen on any free port
            serverSocket = new ServerSocket(0);
            port = serverSocket.getLocalPort();
            System.out.println(ip + " at port number: " + port);

            Thread thread = new Thread(new ListeningThread(serverSocket));
            thread.start();

            // create a scanner so we can read the command-line input
            Scanner scanner = new Scanner(System.in);
            P2 p2 = new P2();

            // check if rebooted
            filePath = "/tmp/lwang3/linda/" + hostName + "/";
            File net = new File(filePath, "nets.txt");

            Broadcast bc = new Broadcast();

            if (net.exists()) {  // current host is rebooted
                System.out.println("Current host is re-booting...");
                // broadcast the net file to get the latest slot table and address book
                bc.rebootCast("reb" + hostName, filePath, hostName);

                lock = true;
                while (lock) {
                    try {
                        Thread.currentThread().sleep(2000);
                    } catch (InterruptedException e) {
                        System.out.println(e);
                    }
                }


                // sync with backup tuples





                // broadcast others






            } else { // current host is newly created

                for (int i = 0; i < slotTable.length; i ++) {
                    slotTable[i] = new Slot(hostName);
                }

                //create the net file and tuple file first
                filePath = "/tmp/lwang3/linda/" + hostName + "/";
                File tuplesO = new File(filePath + "tuples/", "original.txt");
                tuplesO.getParentFile().mkdirs();
                File tuplesB = new File(filePath + "tuples/", "backup.txt");
                File nets = new File(filePath, "nets.txt");

                if(tuplesO.exists()) {
                    tuplesO.delete();
                }
                if(tuplesB.exists()) {
                    tuplesB.delete();
                }

                addressBook = new ArrayList<>();

                // change permission
                try {
                    nets.createNewFile();
                    tuplesO.createNewFile();
                    tuplesB.createNewFile();
                    Runtime.getRuntime().exec("chmod 777 /tmp/lwang3");
                    Runtime.getRuntime().exec("chmod 777 /tmp/lwang3/linda");
                    Runtime.getRuntime().exec("chmod 777 /tmp/lwang3/linda/" + hostName);
                    Runtime.getRuntime().exec("chmod 666 /tmp/lwang3/linda/" + hostName + "/nets.txt");
                    Runtime.getRuntime().exec("chmod 666 /tmp/lwang3/linda/" + hostName + "/tuples");
                    Runtime.getRuntime().exec("chmod 666 /tmp/lwang3/linda/" + hostName + "/tuples/original.txt");
                    Runtime.getRuntime().exec("chmod 666 /tmp/lwang3/linda/" + hostName + "/tuples/backup.txt");

                    new HostManager().flushHostNet(addressBook, filePath, hostName);

                } catch (IOException e) {
                    System.out.println(e);
                }
            }


            while (true) {

                //  prompt for the user's command
                System.out.print(hostName + " linda> ");

                // get their input as a String
                String command = scanner.nextLine();
                if (command.equals("")) {
                    continue;
                }
                p2.parseCommand(command);
            }

        } catch (IOException e) {
            System.out.println(e);
        }
    }

    public void parseCommand (String command) {
        DataProcess dp = new DataProcess();
        Search search = new Search();
        MessageSender ms  = new MessageSender();
        Broadcast bc = new Broadcast();
        ReArranger ra = new ReArranger();
        if (command.length() < 5) {
            System.out.println("Invalid command! Try again...");
            return;
        }
        if (command.substring(0, 3).equals("add")) {
            String hostList = command.substring(3, command.length());
            hostList = hostList.replaceAll("\\s*\\(", "(");
            if (hostList.charAt(0) != '(' || hostList.charAt(hostList.length() - 1) != ')') {
                System.out.println("Invalid User Input! Please add \"()\" for each host." +
                        "Example: add (<host name>, <IP>, <Port>) (<host name>, <IP>, <Port>) (...)");
                return;
            }

            HostManager ad = new HostManager();

            // check if the new hosts reachable
            needACK = ad.checkNewHost(hostList,ip,port);
            if (needACK == -1) {
                return;
            }
            try {
                System.out.println("Checking the hostname of new added hosts...");
                Thread.currentThread().sleep(2000);
            } catch (InterruptedException e) {
                System.out.println(e);
            }
            if (needACK != 0) {
                System.out.println(errorMessage);
                System.out.println("Try again...");
                return;
            }

            oldBackupAddressBook = new ArrayList<>();

            //backup the old address book
            for(Address a : addressBook) {
                oldBackupAddressBook.add(new Address(a.hostName, a.ip, a.port, a.ifAlive));
            }


            // ask for address books belonging to new added host
            ad.requireAddressBook(hostList, ip, port);
            try {
                System.out.println("Adding the new host...");
                Thread.currentThread().sleep(2000);
            } catch (InterruptedException e) {
                System.out.println(e);
            }

            // before the broadcast check if any host name has duplicated
            HashSet<String> nameSet = new HashSet<>();
            nameSet.add(hostName);
            if (!ad.ifDuplicated(addressBook, nameSet)) {
                System.out.println("Same name is found for different IP! Please re-enter....");

                // recover the address book
                addressBook = oldBackupAddressBook;
                return;
            }

            // save the address book to disk net file
            ad.flushHostNet(addressBook, filePath, hostName);

            StringBuilder sb = new StringBuilder("(" + hostName + " " + ip + " " + port + " true)");
            for (Address each: addressBook) {
                sb.append("(" + each.hostName + " " + each.ip + " " + each.port + " " + each.ifAlive + ")");
            }

            // check if any host is not alive, then re-arrange the backup tuple file
            int hostDown = -2;
            boolean ifDown = false;

            for (int i = 0; i < addressBook.size(); i ++) {
                if (!addressBook.get(i).ifAlive) {
                    ifDown = true;
                    hostDown = i;
                    break;
                }
            }

            if (ifDown) { // there is a host is down when adding new hosts, the backup host needs to do re-arrange for this down host
                Address rearrange = addressBook.get((hostDown + 1) % addressBook.size());
                ms.simpleSend("rea" + sb.toString(), rearrange.hostName, addressBook);
            }


            // broadcast everyone in the list to update their address book
            bc.broadcast("add" + sb.toString(), addressBook);


            // sort address book and re-arrange
            addressBook.sort(Address::compareTo);

            new ReArranger().reArrangeAdd(filePath, totalSlot, hostName, slotTable, oldBackupAddressBook, addressBook);

            System.out.println("All hosts have been successfully added!");

        } else if (command.substring(0, 3).equals("del")) {
            command = command.replace("\\s+", "");
            if (command.length() < 8) {
                System.out.println("Invalid User Input!");
                return;
            }
            String hostList = command.substring(6, command.length());
            if (hostList.charAt(0) != '(' || hostList.charAt(hostList.length() - 1) != ')') {
                System.out.println("Invalid User Input! Please add \"()\" for host list to delete." +
                        "Example: delete (<host name 1>, <host name 2>, ...)");
                return;
            }

            HostManager hm = new HostManager();

            // check if duplicated remove same host name
            hostList = hostList.substring(1, hostList.length() - 1).replace(" ", "");
            String[] list = hostList.split(",");
            HashSet<String> set = new HashSet<>();
            for (String each: list) {
                if (set.contains(each)) {
                    System.out.println("Duplicate host name appear in the deleting list! Please re-enter...");
                    return;
                } else {
                    set.add(each);
                }
            }

            // check if the hosts exist in list and if self is deleted
            boolean self = false;
            for (String each: list) {
                if (ms.searchIndex(each, addressBook) == -1) {
                    System.out.println("Host name " + each + "is not in net file! Please re-enter...");
                    return;
                }
                if (each.equals(hostName)) {
                    self = true;
                }
            }

            // check if all hosts got deleted
            if (list.length == addressBook.size()) {
                System.out.println("User tries to delete all hosts, which will cause data lost! Please re-enter...");
                return;
            }

            // broadcast the message of deleted host
            bc.broadcast("del" + hostList, addressBook);

            if (!self) { // current host is not deleted
                hm.deletehosts(filePath, hostName, set, slotTable, addressBook);

            } else { // current host is deleted
                System.out.println("Current host is deleted. Will exit after done with organizing data...");
                ra.reArrangeDelete(filePath, hostName, slotTable, set, addressBook);

                // delete the net file from local
                File file = new File(filePath + "nets.txt");
                file.delete();

                System.exit(0);
            }

        } else if (command.substring(0, 2).equals("rd") || command.substring(0, 2).equals("in") ) {
            String content = command.substring(2, command.length());
            content = content.replaceAll("\\s*\\(", "(");
            if (content.charAt(0) != '(' || content.charAt(content.length() - 1) != ')') {
                System.out.println("Invalid format! (Please add \"()\" outside the data)");
                return;
            }
            content = content.substring(1, content.length() - 1);
            String[] a = new String[] {content};
            boolean[] validity = dp.checkUserInput(a);
            content = a[0];
            if (!validity[0]) {
                System.out.println("Invalid data! Please re-enter...");
                return;
            }
            if (!validity[1]) { // data doesn't have variable
                String[] sa = content.split("\\s*,\\s*");
                StringBuilder sb = new StringBuilder();
                for (String each: sa) {
                    sb.append(each);
                }
                int hostToGet = dp.md5sum(sb.toString(), totalSlot);
                String whoHas = slotTable[hostToGet].hostBelong;
                if (whoHas.equals(hostName)) {
                    String res = search.searchInLocal(content,filePath);
                    if (res == null) { // data has NOT been written in local
                        System.out.println("Waiting until the data can be gotten in local ...");
                    }
                    while (res == null) {
                        try {
                            Thread.currentThread().sleep(1000);
                            res = search.searchInLocal(content,filePath);
                        } catch (InterruptedException e) {
                            System.out.println(e);
                        }
                    }

                    System.out.println("get tuple (" + content + ") on " + hostName);

                    if (command.substring(0, 2).equals("in")) {  // delete the tuple
                        slotTable[hostToGet].tupleSaved = false;
                        bc.broadcast("sl0" + hostToGet, addressBook);
                        search.removeTuple("(" + res + ")", filePath);
                        // send to backup to remove this tuple as well
                        int backup = (ms.searchIndex(hostName, addressBook) + 1) % addressBook.size();
                        ms.simpleSend("bacrem (" + content + ")", addressBook.get(backup).hostName, addressBook);
                        System.out.println("Locally remove the tuple (" + res + ") after offering tuple for \"in\" command.");
                    }

                } else { // data is not in local
                    System.out.println("need to get tuple (" + content + ") on " + whoHas);
                    System.out.println("Waiting until the data can be gotten ...");
                    lock = true;
                    while (lock) {
                        try {
                            ms.send(command.substring(0, 2) + " " + hostName + " " + content, whoHas, addressBook);
                            Thread.currentThread().sleep(1000);
                        } catch (InterruptedException e) {
                            System.out.println(e);
                        }
                    }

                    if (command.substring(0, 2).equals("in")) {
                        ms.simpleSend("orirem (" + content + ")", whoHas, addressBook);
                        slotTable[hostToGet].tupleSaved = false;
                        bc.broadcast("sl0" + hostToGet, addressBook);
                        // send to backup to remove this tuple as well
                        int backup = (ms.searchIndex(whoHas, addressBook) + 1) % addressBook.size();
                        ms.simpleSend("bacrem (" + content + ")", addressBook.get(backup).hostName, addressBook);
                    }

                    System.out.println("get tuple (" + content + ") on " + whoHas);
                }

            } else { // data has variable in it
                System.out.println("Waiting until the data can be gotten ...");
                lock = true;
                while (lock) {
                    // search in local
                    String res = search.searchInLocal(content, filePath);
                    if (res != null) {
                        System.out.println("get tuple (" + res + ") on " + hostName + " " + ip);

                        if (command.substring(0, 2).equals("in")) {  // delete the tuple

                            // update the slot table
                            int hostToGet = dp.md5sum(res, totalSlot);
                            slotTable[hostToGet].tupleSaved = false;
                            bc.broadcast("sl0" + hostToGet, addressBook);

                            search.removeTuple("(" + res + ")", filePath);

                            // send to backup to remove this tuple as well
                            int backup = (ms.searchIndex(hostName, addressBook) + 1) % addressBook.size();
                            ms.simpleSend("bacrem (" + res + ")", addressBook.get(backup).hostName, addressBook);

                            System.out.println("Locally remove the tuple (" + res + ") after offering tuple for \"in\" command.");
                        }
                        return;
                    }
                    // broadcast to search
                    bc.broadcast(command.substring(0, 2) + " " + hostName + " " + content, addressBook);
                    try {
                        Thread.currentThread().sleep(1000);
                    } catch (InterruptedException e) {
                        System.out.println(e);
                    }
                }

                if (command.substring(0, 2).equals("in")) {
                    ms.simpleSend("orirem (" + aws + ")", awsName, addressBook);
                    // send to backup to remove this tuple as well
                    int backup = (ms.searchIndex(awsName, addressBook) + 1) % addressBook.size();
                    ms.simpleSend("bacrem (" + aws + ")", addressBook.get(backup).hostName, addressBook);
                }

                int hostToGet = dp.md5sum(aws, totalSlot);
                slotTable[hostToGet].tupleSaved = false;
                bc.broadcast("sl0" + hostToGet, addressBook);

                System.out.println("get tuple (" + aws + ") on " + awsName);
            }

        } else if (command.substring(0, 3).equals("out")) {
            String content = command.substring(3, command.length());
            content = content.replaceAll("\\s*\\(", "(");
            if (content.charAt(0) != '(' || content.charAt(content.length() - 1) != ')') {
                System.out.println("Invalid format for \"out\" command! (Please add \"()\" outside the data)");
                return;
            }
            content = content.substring(1, content.length() - 1);
            String[] a = new String[] {content};
            boolean[] res = dp.checkUserInput(a);
            content = a[0];
            if (!res[0] || res[1]) {
                System.out.println("Invalid data! Please re-enter...");
                return;
            }
            String[] sa = content.split("\\s*,\\s*");
            StringBuilder sb = new StringBuilder();
            for (String each: sa) {
                sb.append(each);
            }
            int hostToPut = dp.md5sum(sb.toString(), totalSlot);
            String whoHas = slotTable[hostToPut].hostBelong;
            slotTable[hostToPut].tupleSaved = true;
            bc.broadcast("sl1" + hostToPut, addressBook);
            if (whoHas.equals(hostName)) {
                System.out.println("put tuple (" + content + ") on " + hostName);
                search.addNewTuple("(" + content + ")", filePath);
            } else {
                ms.send("out " + content, whoHas, addressBook);
                System.out.println("put tuple (" + content + ") on " + whoHas);
            }

            // send to backup to add this tuple as well
            int backup = (ms.searchIndex(whoHas, addressBook) + 1) % addressBook.size();
            ms.simpleSend("bacsav (" + content + ")", addressBook.get(backup).hostName, addressBook);

        } else {
            System.out.println("Invalid Command! Re-enter...");
        }
    }

    public void parseMessage (String message) {
        MessageSender ms = new MessageSender();
        Search search = new Search();
        HostManager hm = new HostManager();
        ReArranger ra = new ReArranger();

        if (message.substring(0, 3).equals("wro")) { // wrong host name added to list
            String[] infor = message.split("\\s+");
            errorMessage = "Wrong host name for " + infor[2] + ": " + infor[3] + ". Should be " + infor[1];

        } else if (message.substring(0, 3).equals("ack")) { // send back confirm message for correct host name
            needACK --;

        } else if (message.substring(0, 3).equals("sl1")) { // update the slot table
            int index = Integer.parseInt(message.substring(3, message.length()));
            slotTable[index].tupleSaved = true;

        } else if (message.substring(0, 3).equals("reb")) { // be required for updated address book and slot table
            int index = ms.searchIndex(message.substring(3, message.length()), addressBook);
            // send back the address book  already build in




            // send back the slot table





        } else if (message.substring(0, 3).equals("sl0")) { // update the slot table
            int index = Integer.parseInt(message.substring(3, message.length()));
            slotTable[index].tupleSaved = false;

        } else if (message.substring(0, 3).equals("del")) { // delete some hosts
            String deleteList = message.substring(3, message.length());
            String[] list = deleteList.split(",");
            HashSet<String> set = new HashSet<>();

            boolean self = false;
            for (String each: list) {
                set.add(each);
                if (each.equals(hostName)) {
                    self = true;
                    break;
                }
            }

            if (!self) { // current host is not deleted
                hm.deletehosts(filePath, hostName, set, slotTable, addressBook);

            } else { // current host is deleted
                System.out.println("Current host is deleted. Will exit after done with organizing data...");
                ra.reArrangeDelete(filePath, hostName, slotTable, set, addressBook);

                // delete the net file from local
                File file = new File(filePath + "nets.txt");
                file.delete();

                System.exit(0);
            }

        } else if (message.substring(0, 3).equals("req")) { // received a message asking for address book
            String[] content = message.split("\\s+");
            StringBuilder sb = new StringBuilder("add(" + hostName + " " + ip + " " + port + " true)");
            for (Address each: addressBook) {
                sb.append("(" + each.hostName + " " + each.ip + " " + each.port + " " + each.ifAlive + ")");
            }
            ms.directSend(sb.toString(), content[1], Integer.parseInt(content[2]));

        } else if (message.substring(0, 3).equals("con")) { // received confirm message for host name
            String[] infor = message.split("\\s+");
            String nameConfirm = infor[1];
            if (!nameConfirm.equals(hostName)) {
                ms.directSend("wrong " + hostName + " " + ip + " " + port, infor[2], Integer.parseInt(infor[3]));
            } else {
                ms.directSend("ack", infor[2], Integer.parseInt(infor[3]));
            }

        }  else if (message.substring(0, 3).equals("aws")) {
            awsName = message.split("\\s+")[1];
            aws = message.substring(5 + awsName.length(), message.length());
            lock = false;

        } else if (message.substring(0, 3).equals("add") || message.substring(0, 3).equals("res") || message.substring(0, 3).equals("rea")) {
            String content = message.substring(3, message.length());
            String[] hostList = content.split("\\)");
            ArrayList<String> addingList = new ArrayList<>();

            if (message.substring(0, 3).equals("add") || message.substring(0, 3).equals("rea")) {
                oldBackupAddressBook = new ArrayList<>();
                for(Address a : addressBook) {
                    oldBackupAddressBook.add(new Address(a.hostName, a.ip, a.port, a.ifAlive));
                }
            }

            for (String each: hostList) {
                each = each.substring(1, each.length());
                String[] infor = each.split("\\s+");
                String curName  = infor[0];
                String curIP = infor[1];
                int curPort = Integer.parseInt(infor[2]);
                boolean ifAlive = Boolean.parseBoolean(infor[3]);

                //remove the duplicate
                if (!curName.equals(hostName)) {
                    for (Address address: addressBook) {
                        if (address.hostName.equals(curName) && address.ip.equals(curIP) && address.port == curPort) {
                            continue;
                        } else {
                            addressBook.add(new Address(curName, curIP, curPort, ifAlive));
                            addingList.add(curName + " " + curIP + " " + curPort);
                        }
                    }
                }
            }

            HostManager ad = new HostManager();
            ad.flushHostNet(addressBook, filePath, hostName);
            System.out.println("Following hosts have been successfully added!");
            System.out.println(addingList.toString());

            if (message.substring(0, 3).equals("res")) {
                return; // skip re-arrange
            }

            // sort new address book
            addressBook.sort(Address::compareTo);

            // if the message is "add" host, then re-arrange the original file
            if (message.substring(0, 3).equals("add")) {
                new ReArranger().reArrangeAdd(filePath, totalSlot, hostName, slotTable, oldBackupAddressBook, addressBook);
            }

            // if the message is "rea"rrange, then re-arrange the original file and the backup file for down host as well
            if (message.substring(0, 3).equals("rea")) {
                new ReArranger().reArrangeRea(filePath, totalSlot, hostName, slotTable, oldBackupAddressBook, addressBook);
            }


        } else if (message.substring(0, 3).equals("flu")) { // save the tuple to original
            String content = message.substring(4, message.length());
            search.flush(content, filePath + "tuples/backup.txt");
            System.out.println("Reallocated tuples in format of (slot#-> " + content + ") in local backup due to deleting/adding host.");
        } else if (message.substring(0, 3).equals("ori") || message.substring(0, 3).equals("bac")) {

            String accessFile;
            if (message.substring(0, 3).equals("ori")) {
                accessFile = filePath + "tuples/original.txt"; // operation on original file
            } else {
                accessFile = filePath + "tuples/backup.txt"; // operation on backup file
            }
            message = message.substring(3, message.length());

            if (message.substring(0, 2).equals("rd") || message.substring(0, 2).equals("in")) {
                String res = message.split("\\s+")[1];
                String content = message.substring(4 + res.length(), message.length());
                String aws = search.searchInLocal(content, accessFile);
                if (aws != null) {
                    ms.send("aws " + hostName + " " + aws, res, addressBook);
                    System.out.println("Try to offer tuple (" + aws + ") to " + res);
                }

            } else if (message.substring(0, 3).equals("rem")) { // delete this tuple
                String tuple = message.substring(4, message.length());
                search.removeTuple(tuple, accessFile);
                System.out.println("Locally remove the tuple " + tuple + " after offering tuple for \"in\" command.");

            } else if (message.substring(0, 3).equals("out")) {
                String content = message.substring(4, message.length());
                search.addNewTuple("(" + content + ")", accessFile);
                System.out.println("Save tuple (" + content + ") in local.");

            } else if (message.substring(0, 3).equals("sav")) { // save the tuple to original
                String content = message.substring(4, message.length());
                search.addNewTuple(content, accessFile);
                System.out.println("Reallocated tuples in format of (slot#-> " + content + ") in local " + accessFile + " due to deleting/adding host.");

            } else {
                System.out.println("Received a unpredicted message: " + message);
            }
        } else {
            System.out.println("Received a unpredicted message: " + message);
        }
    }
}