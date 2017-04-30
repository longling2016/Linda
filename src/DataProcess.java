import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;

/**
 * Created by longlingwang on 4/8/17.
 */
public class DataProcess {
    public boolean[] checkUserInput(String[] a) {
        String s = a[0];
        boolean[] result = new boolean[2];
        if (s.length() == 0) {
            System.out.println("No data entered!");
            return result;
        }

        // remove unnecessary white space  data if any
        int i = 0;
        while (i < s.length() - 1 && s.charAt(i) == ' ') {
            i ++;
        }
        int j = s.length() - 1;
        while (j >= 0 && s.charAt(j) == ' ') {
            j --;
        }

        if (i > j) {
            return result;
        }

        s = s.substring(i, j + 1);

        String[] tuple = s.split("\\s*,\\s*");
        if (tuple.length == 0) {
            return result;
        }

        for (int p = 0; p < tuple.length; p ++) {
            String each = tuple[p];
            if (each.length() == 0) {
                return result;
            }

            if (each.charAt(0) == '"' && each.charAt(each.length() - 1) == '"') {
                continue;
            }
            if (isInteger(each)) {
                tuple[p] = String.valueOf(Integer.parseInt(each));
                continue;
            }
            if (isFloat(each)) {
                tuple[p] = String.valueOf(Float.parseFloat(each));
                continue;
            }
            if (each.charAt(0) == '?') {
                each = each.replace(" ", "");
                result[1] = true;
                int length = each.length();
                if (length < 6) {
                    return result;
                }
                if (length == 6) {
                    if (each.substring(length - 4, length).equals(":int")) {
                        continue;
                    } else {
                        return result;
                    }
                }
                if (each.substring(length - 4, length).equals(":int") ||
                        each.substring(length - 6, length).equals(":float") ||
                        each.substring(length - 7, length).equals(":String")) {
                    continue;
                }
            }
            return result;
        }
        result[0] = true;
        StringBuilder sb = new StringBuilder();
        for (int p = 0; p < tuple.length - 1; p ++) {
            sb.append(tuple[p] + ", ");
        }
        sb.append(tuple[tuple.length - 1]);
        a[0] = sb.toString();
        return result;
    }

    public boolean isInteger(String s) {
        try {
            Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return false;
        } catch (NullPointerException e) {
            return false;
        }
        return true;
    }

    public boolean isFloat(String s) {
        boolean b = false;
        for (int i = 0; i < s.length(); i ++) {
            if (s.charAt(i) == '.') {
                b = true;
                break;
            }
        }
        if (!b) {
            return false;
        }
        try {
            Float.parseFloat(s);
        } catch (NumberFormatException e) {
            return false;
        } catch (NullPointerException e) {
            return false;
        }
        return true;
    }

    public int md5sum(String s, int numOfHost) {
        if (numOfHost == 0) {
            System.out.println("No other host has been added!");
            return 0;
        }
        try {
            MessageDigest m = MessageDigest.getInstance("MD5");
            m.reset();
            m.update(s.getBytes());
            byte[] digest = m.digest();
            BigInteger afterHash = new BigInteger(1, digest);
            BigInteger numToMod = BigInteger.valueOf(numOfHost);
            BigInteger res = afterHash.mod(numToMod);
            return res.intValue();

        } catch (NoSuchAlgorithmException e) {
            System.out.println(e);
            return -1;
        }
    }

    public String getBackup (String filePath) {
        StringBuilder sb = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath + "tuples/backup.txt"));
            String line = br.readLine();
            while ((line = br.readLine()) != null) {
                sb.append("(" + line + ")");
            }
            br.close();
            return sb.toString();
        } catch (IOException e) {
            System.out.println(e);
        }
        return null;
    }

    public String getOriginal (String filePath) {
        StringBuilder sb = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath + "tuples/original.txt"));
            String line;
            while ((line = br.readLine()) != null) {
                sb.append("(" + line + ")");
            }
            br.close();
            return sb.toString();
        } catch (IOException e) {
            System.out.println(e);
        }
        return null;
    }




//    public void updateSlotTable (String tuples, Slot[] slotTable) {
//        String[] tupleList = tuples.substring(1, tuples.length() - 1).split("\\)\\(");
//        for (String each : tupleList) {
//            int slotIndex = Integer.parseInt(each.split("->")[0]);
//            slotTable[slotIndex].tupleSaved = false;
//        }
//    }
}