package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.BlockingQueue;

/**
 * Listens on the LISTENING_PORT and adds the received messages to a message queue.
 * The messages in the queue are then processed by the Co-ordinator
 */
public class Listener {

    private static final String TAG = Listener.class.getSimpleName();
    private static final int LISTENING_PORT = 10000;
    private BlockingQueue<String> receivedMessageQueue;


    Listener(BlockingQueue receivedMessageQueue) {
        this.receivedMessageQueue = receivedMessageQueue;
        new Thread(new ListenerRunnable()).start();
    }

    private class ListenerRunnable implements Runnable {
        final String TAG = ListenerRunnable.class.getSimpleName();
        @Override
        public void run() {

            ServerSocket listenerSocket = null;
            try {
                listenerSocket = new ServerSocket(LISTENING_PORT);
            } catch (IOException e) {
                Log.e(TAG, "Can't create a ServerSocket", e);
            }

            //noinspection InfiniteLoopStatement
            while (true) {
                try {
                    Socket server = listenerSocket.accept();
                    DataInputStream in = new DataInputStream(server.getInputStream());
                    String mesRecvd = in.readUTF();
                    server.close();
                    // add the message to the coordinator's queue
                    try {
                        receivedMessageQueue.put(mesRecvd);
                    } catch (InterruptedException e) {
                        Log.e(TAG, "Unable to add message to the coordinator's queue.", e);
                    }
                } catch (SocketTimeoutException s) {
                    Log.e(TAG, "Receiver timed out!");
                } catch (IOException e) {
                    Log.e(TAG, Log.getStackTraceString(e));
                }
            }
        }
    }

}
