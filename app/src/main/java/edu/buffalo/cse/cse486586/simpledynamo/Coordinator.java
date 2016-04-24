package edu.buffalo.cse.cse486586.simpledynamo;


import android.content.Context;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Coordinator {

    private static final String LOG_TAG = Coordinator.class.getSimpleName();
    final int MY_ID;
    final int MY_PORT;
    final String MY_HASH;
    final int [] NODE_IDS = {5554, 5556, 5558, 5560, 5562};
    final List<Node> NODE_LIST;
    final List<Node> PREFERENCE_LIST;

    private Context mContext;

    HashMap<Integer, Object> messageMap;

    // Coordinator is singleton
    private static Coordinator instance;

    // used to send messages in a new thread
    Sender mSender;
    // used to store the messages received by the listener
    BlockingQueue<String> receivedMessageQueue;

    // constants for Amazon Dynamo
    private static class Dynamo {
        static final int N = 3;     // number of replicas
        static final int R = 2;     // number of reader
        static final int W = 2;     // number of writers
    }

    class Node implements Comparable<Node>{
        final int id;
        final int port;
        final String hash;
        Node(int id) {
            this.id = id;
            port = id * 2;
            hash = genHash(Integer.toString(id));
        }

        @Override
        public boolean equals(Object o) {
            return hash.equals(((Node)o).hash);
        }

        @Override
        public int compareTo(Node another) {
            return hash.compareTo(another.hash);
        }

        @Override
        public String toString() {
            return "Id: " + id + "\nPort: " +port +"\nHash: " +hash;
        }
    }

    private Coordinator(Context context) {
        if (context == null) {
            throw new RuntimeException("FATAL ERROR: " +
                    "Context is null while creating coordinator");
        }

        this.mContext = context;

        // initialize my sending port
        TelephonyManager tel = (TelephonyManager) mContext.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        MY_ID = (Integer.parseInt(portStr));
        MY_PORT = MY_ID * 2;
        MY_HASH = genHash(Integer.toString(MY_ID));
        Log.i(LOG_TAG, "My Id: " + MY_ID + "\nMy Port: " +MY_PORT +"\nMY Hash: " +MY_HASH);

        // start a Listener on port 10000,
        // listener adds the messages received to the receivedMessageQueue
        receivedMessageQueue = new LinkedBlockingQueue();
        new Listener(receivedMessageQueue);

        // create a sender, used to send messages in a new thread
        mSender = new Sender();

        // messageMap to keep track of all the messages sent
        messageMap = new HashMap<Integer, Object>();

        // initialize the other Nodes list
        NODE_LIST = new ArrayList<Node>();
        for(int i = 0; i < NODE_IDS.length; i++) {
            NODE_LIST.add(new Node(NODE_IDS[i]));
        }
        Collections.sort(NODE_LIST);
        Log.i(LOG_TAG, "NODE_LIST:");
        for(int i = 0; i < NODE_LIST.size(); i++) {
            Log.i(LOG_TAG, NODE_LIST.get(i).toString());
        }

        // initialize the preference list
        PREFERENCE_LIST = new ArrayList<Node>();
        int i;
        for(i = 0; i < NODE_LIST.size(); i++) {
            if(NODE_LIST.get(i).hash.equals(MY_HASH))
                break;
        }
        if(i == NODE_LIST.size())
            throw new RuntimeException("FATAL ERROR: My Node id not in node list.");
        for(int j = i; j < i+Dynamo.N; j++) {
            PREFERENCE_LIST.add(NODE_LIST.get(j%NODE_LIST.size()));
        }
        Log.i(LOG_TAG, "PREFERENCE_LIST:");
        for(int j = 0; j < PREFERENCE_LIST.size(); j++) {
            Log.i(LOG_TAG, PREFERENCE_LIST.get(j).toString());
        }
    }

    // this should be used only for initialization
    static Coordinator getInstance(Context context) {
        if(instance != null){
            throw new RuntimeException("FATAL ERROR: " +
                    "Coordinator is already initialized.");
        }
        instance = new Coordinator(context);
        return instance;
    }

    // this should be used for getting the instance after initialization
    static Coordinator getInstance() {
        if(instance == null){
            throw new RuntimeException("FATAL ERROR: " +
                    "Coordinator is not initialized.");
        }
        return instance;
    }

    void start(){
        Log.i(LOG_TAG, "Starting to process messages.");
        while(true){
            try {
                String message = receivedMessageQueue.take();
                Message messageObject = Utility.buildMessageObjectFromJson(message);
                Log.v(LOG_TAG, "Handling message: " +messageObject);
//                handleMessage(messageObject);
            } catch (InterruptedException e) {
                Log.e(LOG_TAG, "Exception: Message Consumer was interrupted.", e);
            }
        }
    }

    private static String genHash(String input)  {
        MessageDigest sha1 = null;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            Log.e(LOG_TAG, "Exception: SHA-1 algorithm not found", e);
        }
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    Node findCoordinatingNode(String key) {
        String keyHash = genHash(key);
        for (int i =0; i < NODE_LIST.size(); i++) {
            Node currentNode = NODE_LIST.get(i);
            if(keyHash.compareTo(currentNode.hash) <= 0) {
                return currentNode;
            }
        }
        return NODE_LIST.get(0);
    }

    void handleMessage(Message handledMessage) {

    }
}
