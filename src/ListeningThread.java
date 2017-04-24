import java.io.DataInputStream;
import java.io.IOException;
import java.net.*;
import java.net.Socket;

/**
 * Created by longlingwang on 4/8/17.
 */
public class ListeningThread implements Runnable {
    ServerSocket ss;

    public ListeningThread(ServerSocket ss) {
        this.ss = ss;
    }

    public void run() {
        DataInputStream dIn = null;
        Socket socket;
        P2 p2 = new P2();
        try {
            while (true) {
                socket = ss.accept();
                dIn = new DataInputStream(socket.getInputStream());

                String message = dIn.readUTF();

                p2.parseMessage(message);

            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (dIn != null) {
                try {
                    dIn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
