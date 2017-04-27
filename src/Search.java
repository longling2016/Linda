import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Created by longlingwang on 4/8/17.
 */
public class Search {
    public String searchInLocal(String target, String filePath) {
        String[] tuple = target.split("\\s*,\\s*");
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            DataProcess dp = new DataProcess();
            String line;
            String[] pre;
            while ((line = br.readLine()) != null) {
                pre = line.split("->");
                if (pre.length < 2) {
                    continue;
                }
                String[] curline = pre[1].split("\\s*,\\s*");
                if (tuple.length != curline.length) {
                    continue;
                }
                boolean flag = true;
                int i;
                for (i = 0; i < tuple.length && i < curline.length; i++) {
                    String curTagert = tuple[i];
                    String curCompare = curline[i];

                    if (curTagert.charAt(0) == '?') {
                        curTagert = curTagert.replace(" ", "");
                        int length = curTagert.length();
                        if (curTagert.substring(length - 3, length).equals("int") && !dp.isInteger(curCompare)) {
                            flag = false;
                            break;
                        }
                        if (curTagert.substring(length - 3, length).equals("oat") && !dp.isFloat(curCompare)) {
                            flag = false;
                            break;
                        }
                        if (curTagert.substring(length - 3, length).equals("ing")) {
                            if (curCompare.charAt(0) != '"' || curCompare.charAt(curCompare.length() - 1) != '"') {
                                flag = false;
                                break;
                            }
                        }
                    } else {  // curTagert.charAt(0) != '?'
                        if (curCompare.charAt(0) == '"' && curTagert.charAt(0) == '"' && curCompare.equals(curTagert)) {
                            continue;
                        } else if (dp.isInteger(curCompare) && dp.isInteger(curTagert) && Integer.parseInt(curCompare) == Integer.parseInt(curTagert)) {
                            continue;
                        } else if (dp.isFloat(curCompare) && dp.isFloat(curTagert) &&
                                Math.abs(Float.parseFloat(curCompare) - Float.parseFloat(curTagert)) <= 0.00000001) {
                            continue;
                        } else {
                            flag = false;
                            break;
                        }
                    }
                }
                if (flag && i == curline.length) {
                    br.close();
                    return line;
                }
            }
            br.close();
        } catch (IOException e) {
            System.out.println(e);
        }
        return null;
    }

    public void removeTuple(String tuple, String filePath) {
        System.out.println("remove tuple: " + tuple);
        File inputFile = new File(filePath);
        String folder = inputFile.getParent();
        File tempFile = new File(folder + "/tempFile.txt");
        tuple = tuple.substring(1, tuple.length() - 1);
        ArrayList<String> removeList = new ArrayList<>();
        removeList.addAll(Arrays.asList(tuple.split("\\)\\(")));

        try {
            BufferedReader reader = new BufferedReader(new FileReader(inputFile));
            BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));

            String currentLine;

            while ((currentLine = reader.readLine()) != null) {
                // trim newline when comparing with lineToRemove
                String trimmedLine = currentLine.trim();
                if (removeList.contains(trimmedLine)) {
                    removeList.remove(trimmedLine);
                    continue;
                }
                writer.write(currentLine + System.getProperty("line.separator"));
            }
            writer.close();
            reader.close();
            tempFile.renameTo(inputFile);
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    public void addNewTuple(String tuple, String filePath) {
        try {
            FileWriter fw = new FileWriter(filePath, true);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw);
            tuple = tuple.substring(1, tuple.length() - 1);
            String[] tuples = tuple.split("\\)\\(");
            for (String each : tuples) {
                out.println(each);
            }
            out.close();
            bw.close();
            fw.close();
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    public void flush(String content, String filePath) {
        try {
            String[] infor = content.split("::");
            String backupForWho = infor[0];
            String tuples = infor[1];

            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String line = br.readLine();
            br.close();

            FileWriter fw;
            if (line.equals(backupForWho)) {
                fw = new FileWriter(filePath, true);
            } else {
                fw = new FileWriter(filePath);
            }
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw);
            tuples = tuples.substring(1, tuples.length() - 1);
            String[] tupleList = tuples.split("\\)\\(");
            for (String each : tupleList) {
                out.println(each);
            }
            out.close();
            bw.close();
            fw.close();
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    public void flushBackup(String content, String filePath) {
        try {
            String[] infor = content.split("::");
            System.out.println(content);

            String backupForWho = infor[0];
            FileWriter fw = new FileWriter(filePath);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw);
            out.println(backupForWho);
            if (infor.length == 1) {
                out.close();
                bw.close();
                fw.close();
                return;
            }
            String tuples = infor[1];
            tuples = tuples.substring(1, tuples.length() - 1);
            String[] tupleList = tuples.split("\\)\\(");

            for (String each : tupleList) {
                out.println(each);
            }
            out.close();
            bw.close();
            fw.close();
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    public void sync(String content, String filePath) {
        try {
            content = content.substring(1, content.length() - 1);
            String[] tuples = content.split("\\)\\(");
            FileWriter fw = new FileWriter(filePath);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw);
            for (String each : tuples) {
                out.println(each);
            }
            out.close();
            bw.close();
            fw.close();
        } catch (IOException e) {
            System.out.println(e);
        }
    }
}