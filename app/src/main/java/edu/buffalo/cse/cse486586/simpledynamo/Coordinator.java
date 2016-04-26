package edu.buffalo.cse.cse486586.simpledynamo;


import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.telephony.TelephonyManager;
import android.util.JsonWriter;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.StringWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Coordinator {

    private static final String LOG_TAG = Coordinator.class.getSimpleName();
    final int MY_ID;
    final int MY_PORT;
    final String MY_HASH;
    final int [] NODE_IDS = {5554, 5556, 5558, 5560, 5562};
    final List<Node> NODE_LIST;
    final Map<Integer, Node> NODE_MAP;
    final List<Node> PREFERENCE_LIST;

    private Context mContext;

    Map<Integer, Object> messageMap;

    // Coordinator is singleton
    private static Coordinator instance;

    // used to send messages in a new thread
    Sender mSender;
    // used to store the messages received by the listener
    BlockingQueue<String> receivedMessageQueue;

    // constants for Amazon Dynamo
    private static class DynamoContract {
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
            return o instanceof Node && hash.equals(((Node)o).hash);
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
        receivedMessageQueue = new LinkedBlockingQueue<String>();
        new Listener(receivedMessageQueue);

        // create a sender, used to send messages in a new thread
        mSender = new Sender();

        // messageMap to keep track of all the messages sent
        messageMap = Collections.synchronizedMap(new HashMap<Integer, Object>());

        // initialize the Nodes list, in ascending order of their hash
        NODE_LIST = new ArrayList<Node>();
        for(int node_id : NODE_IDS) {
            NODE_LIST.add(new Node(node_id));
        }
        Collections.sort(NODE_LIST);
        Log.i(LOG_TAG, "NODE_LIST:");
        for(Node node : NODE_LIST) {
            Log.i(LOG_TAG, node.toString());
        }

        // initialize the Nodes map, used for id to node mapping
        Map<Integer, Node>  temp = new HashMap<Integer, Node>();
        for(Node node : NODE_LIST) {
            temp.put(node.id, node);
        }
        NODE_MAP = Collections.unmodifiableMap(temp);

        // initialize the preference list
        PREFERENCE_LIST = getPreferenceList(Integer.toString(MY_ID));
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
                // TODO handle message should go to a new thread
                handleMessage(message);
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

    boolean amITheCoordinator(String key) {
        Node coordinatingNode = findCoordinatingNode(key);
        return coordinatingNode.id == MY_ID? true : false;
    }

    List<Node> getPreferenceList(String key) {
        List<Node> preferenceList = new ArrayList<Node>();
        Node coordinatingNode = findCoordinatingNode(key);
        int i;
        for(i = 0; i < NODE_LIST.size(); i++) {
            if(NODE_LIST.get(i).equals(coordinatingNode))
                break;
        }
        if(i == NODE_LIST.size())
            throw new RuntimeException("FATAL ERROR: Node not present in NodeList. " +
                                            "This shouldn't happen.");
        for(int j = i; j < i+ DynamoContract.N; j++) {
            preferenceList.add(NODE_LIST.get(j%NODE_LIST.size()));
        }
        return preferenceList;
    }

    void handleMessage(String message) {
        // this is a new message object
        Message handledMessage = Utility.buildMessageObjectFromJson(message);
        Log.v(LOG_TAG, "Handling message: " +handledMessage);
        switch (handledMessage.type) {
            case MessageContract.Type.MSG_INSERT_INITIATE_REQUEST: {
                String [] keyValueContext = getKeyValueContext(handledMessage.content);
                // send insert commands to the preference list
                List<Node> preferenceList = getPreferenceList(keyValueContext[0]);
                Message insertMessage = new Message(handledMessage);
                insertMessage.type = MessageContract.Type.MSG_INSERT_REQUEST;
                insertMessage.id = MessageContract.messageCounter++;
                insertMessage.coordinatorId = MY_ID;
                for (Node node : preferenceList) {
                    mSender.sendMessage(Utility.convertMessageToJSON(insertMessage),
                            node.port);
                }

                // create a blocking queue to keep track of the responses
                LinkedBlockingQueue<Message> responseQueue = new LinkedBlockingQueue<Message>();
                messageMap.put(insertMessage.id, responseQueue);

                // poll till we receive all responses or timeout
                List<Message> responses = new ArrayList<Message>();
                for (int i = 0; i < DynamoContract.W; i++) {
                    try {
                        responses.add(responseQueue.poll(
                                MessageContract.RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS));
                    } catch (InterruptedException e) {
                        Log.e(LOG_TAG, "INSERT: Interrupted " +
                                "while waiting for response to: " + insertMessage, e);
                    }
                }
                // remove the message from the messageMap
                messageMap.remove(insertMessage.id);

                // check if we received enough responses and send a response to the source
                if (responses.size() < DynamoContract.W) {
                    Log.e(LOG_TAG, "INSERT: Received only  " + responses.size() + "responses" +
                            " for message: " + insertMessage);
                } else {
                    handledMessage.type = MessageContract.Type.MSG_INSERT_INITIATE_RESPONSE;
                    handledMessage.coordinatorId = MY_ID;
                    mSender.sendMessage(Utility.convertMessageToJSON(handledMessage),
                            NODE_MAP.get(handledMessage.sourceId).port);
                }
            }
            break;
            case MessageContract.Type.MSG_INSERT_INITIATE_RESPONSE: {
                // notify that we have received the response
                Message sentMessage = (Message) messageMap.get(handledMessage.id);
                if(sentMessage != null) {
                    messageMap.put(handledMessage.id, handledMessage);
                    synchronized (sentMessage) {
                        notify();
                    }
                }
            }
            break;
            case MessageContract.Type.MSG_INSERT_REQUEST: {
                String [] keyValueContext = getKeyValueContext(handledMessage.content);
                // insert into my content provider
                ContentValues cv = new ContentValues();
                cv.put(DatabaseContract.DynamoEntry.COLUMN_KEY, keyValueContext[0]);
                cv.put(DatabaseContract.DynamoEntry.COLUMN_VALUE, keyValueContext[1]);
                cv.put(DatabaseContract.DynamoEntry.COLUMN_CONTEXT, keyValueContext[2]);
                mContext.getContentResolver().insert(DatabaseContract.DynamoEntry.NODE_URI, cv);

                // send a response to the coordinator
                handledMessage.type = MessageContract.Type.MSG_INSERT_RESPONSE;
                mSender.sendMessage(Utility.convertMessageToJSON(handledMessage),
                        handledMessage.coordinatorId);
            }
            break;
            case MessageContract.Type.MSG_INSERT_RESPONSE: {
                // add the response to the response queue
                LinkedBlockingQueue responseQueue = (LinkedBlockingQueue<Message>)
                        messageMap.get(handledMessage.id);
                try {
                    if(responseQueue != null) {
                        responseQueue.put(handledMessage);
                    }
                } catch (InterruptedException e) {
                    Log.e(LOG_TAG, "INSERT: Interrupted " +
                            "while adding response to queue." + handledMessage, e);
                }
            }
            break;
            case MessageContract.Type.MSG_QUERY_INITIATE_REQUEST: {
                String key = handledMessage.content;
                if(key.equals("*")) {
                    // send "@" queries to all nodes
                    Message queryMessage = new Message(handledMessage);
                    queryMessage.type = MessageContract.Type.MSG_QUERY_REQUEST;
                    queryMessage.id = MessageContract.messageCounter++;
                    queryMessage.coordinatorId = MY_ID;
                    for (Node node : NODE_LIST) {
                        mSender.sendMessage(Utility.convertMessageToJSON(queryMessage),
                                node.port);
                    }

                    // create a blocking queue to keep track of the responses
                    LinkedBlockingQueue<Message> responseQueue = new LinkedBlockingQueue<Message>();
                    messageMap.put(queryMessage.id, responseQueue);

                    // poll till we receive all responses
                    List<Message> responses = new ArrayList<Message>();
                    for (int i = 0; i < NODE_LIST.size(); i++) {
                        try {
                            responses.add(responseQueue.poll(
                                    MessageContract.RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS));
                        } catch (InterruptedException e) {
                            Log.e(LOG_TAG, "INSERT: Interrupted " +
                                    "while waiting for response to: " + queryMessage, e);
                        }
                    }
                    // remove the message from the messageMap
                    messageMap.remove(queryMessage.id);

                    // reconcile the responses and send a response to the source
                    handledMessage.type = MessageContract.Type.MSG_QUERY_INITIATE_RESPONSE;
                    handledMessage.coordinatorId = MY_ID;
                    handledMessage.content = reconcileResponses(responses);
                    mSender.sendMessage(Utility.convertMessageToJSON(handledMessage),
                            NODE_MAP.get(handledMessage.sourceId).port);
                }
                else {
                    // send query to the preference list
                    List<Node> preferenceList = getPreferenceList(key);
                    Message queryMessage = new Message(handledMessage);
                    queryMessage.type = MessageContract.Type.MSG_QUERY_REQUEST;
                    queryMessage.id = MessageContract.messageCounter++;
                    queryMessage.coordinatorId = MY_ID;
                    for (Node node : preferenceList) {
                        mSender.sendMessage(Utility.convertMessageToJSON(queryMessage),
                                node.port);
                    }

                    // create a blocking queue to keep track of the responses
                    LinkedBlockingQueue<Message> responseQueue = new LinkedBlockingQueue<Message>();
                    messageMap.put(queryMessage.id, responseQueue);

                    // poll till we receive all responses or timeout
                    List<Message> responses = new ArrayList<Message>();
                    for (int i = 0; i < DynamoContract.R; i++) {
                        try {
                            responses.add(responseQueue.poll(
                                    MessageContract.RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS));
                        } catch (InterruptedException e) {
                            Log.e(LOG_TAG, "INSERT: Interrupted " +
                                    "while waiting for response to: " + queryMessage, e);
                        }
                    }
                    // remove the message from the messageMap
                    messageMap.remove(queryMessage.id);

                    // check if we received enough responses and send a response to the source
                    if (responses.size() < DynamoContract.R) {
                        Log.e(LOG_TAG, "INSERT: Received only  " + responses.size() + "responses" +
                                " for message: " + queryMessage);
                    } else {
                        handledMessage.type = MessageContract.Type.MSG_QUERY_INITIATE_RESPONSE;
                        handledMessage.coordinatorId = MY_ID;
                        handledMessage.content = reconcileResponses(responses);
                        mSender.sendMessage(Utility.convertMessageToJSON(handledMessage),
                                NODE_MAP.get(handledMessage.sourceId).port);
                    }
                }
            }
            break;
            case MessageContract.Type.MSG_QUERY_INITIATE_RESPONSE: {
                // notify that we have received the response
                Message sentMessage = (Message) messageMap.get(handledMessage.id);
                if(sentMessage != null) {
                    messageMap.put(handledMessage.id, handledMessage);
                    synchronized (sentMessage) {
                        notify();
                    }
                }
            }
            break;
            case MessageContract.Type.MSG_QUERY_REQUEST: {
                // setup the query parameters
                String selection;
                String [] selectionArgs;
                if(handledMessage.content.equals("*"))
                {
                    selection = null;
                    selectionArgs = null;
                }
                else {
                    selection = "key=?";
                    selectionArgs = new String[] {handledMessage.content};
                }
                // query my content provider
                Cursor result = mContext.getContentResolver().query(DatabaseContract.DynamoEntry.NODE_URI,
                        null, selection, selectionArgs, null);
                // send a response to the coordinator
                handledMessage.type = MessageContract.Type.MSG_QUERY_RESPONSE;
                handledMessage.content = Utility.convertCursorToString(result);
                mSender.sendMessage(Utility.convertMessageToJSON(handledMessage),
                        handledMessage.coordinatorId);
            }
            break;
            case MessageContract.Type.MSG_QUERY_RESPONSE: {
                // add the response to the response queue
                LinkedBlockingQueue responseQueue = (LinkedBlockingQueue<Message>)
                        messageMap.get(handledMessage.id);
                try {
                    if(responseQueue != null) {
                        responseQueue.put(handledMessage);
                    }
                } catch (InterruptedException e) {
                    Log.e(LOG_TAG, "INSERT: Interrupted " +
                            "while adding response to queue." + handledMessage, e);
                }
            }
            break;
            default:
            Log.e(LOG_TAG, "Unknown message type.");
        }
    }
    String [] getKeyValueContext(String text) {
        String [] keyValueContext = new String[3];
        try {
            JSONObject JSONText = new JSONObject(text);
            keyValueContext[0] = JSONText.getString(MessageContract.Field.MSG_CONTENT_KEY);
            keyValueContext[1] = JSONText.getString(MessageContract.Field.MSG_CONTENT_VALUE);
            keyValueContext[2] = JSONText.getString(MessageContract.Field.MSG_CONTENT_CONTEXT);
        }
        catch (JSONException e) {
            Log.e(LOG_TAG, "Improper JSON format in getKeyValueContext()");
        }
        return keyValueContext;
    }

    String reconcileResponses(List<Message> responses) {
        HashMap<String, JSONObject> keyMap = new HashMap<String, JSONObject>();
        for(Message response : responses) {
            try {
                JSONArray messageJSON = new JSONArray(response.content);
                for(int i = 0; i < messageJSON.length(); i++){
                    JSONObject currentObject = messageJSON.getJSONObject(i);
                    String currentKey = currentObject.getString(MessageContract.Field.MSG_CONTENT_KEY);
                    String currentValue = currentObject.getString(MessageContract.Field.MSG_CONTENT_VALUE);
                    int currentVersion = Integer.parseInt(currentObject.getString(
                            MessageContract.Field.MSG_CONTENT_CONTEXT));
                    JSONObject inMapObject = keyMap.get(currentKey);
                    if(inMapObject == null ||
                            Integer.parseInt(inMapObject.getString(
                                    MessageContract.Field.MSG_CONTENT_CONTEXT)) < currentVersion) {
                        keyMap.put(currentKey, currentObject);
                    }
                }
            } catch (JSONException e) {
                Log.e(LOG_TAG, "Exception: Improper Message Format.", e);
            }
        }

        // put the values in the keyMap to a JSONArray
        StringWriter sWriter = new StringWriter();
        JsonWriter jWriter = new JsonWriter(sWriter);
        try {
            jWriter.beginArray();
            for(Map.Entry<String, JSONObject> entry : keyMap.entrySet())
            {
                JSONObject currentObject = entry.getValue();
                String currentKey = currentObject.getString(MessageContract.Field.MSG_CONTENT_KEY);
                String currentValue = currentObject.getString(MessageContract.Field.MSG_CONTENT_VALUE);
                String currentVersion = currentObject.getString(MessageContract.Field.MSG_CONTENT_CONTEXT);
                jWriter.beginObject();
                jWriter.name(MessageContract.Field.MSG_CONTENT_KEY).value(currentKey);
                jWriter.name(MessageContract.Field.MSG_CONTENT_VALUE).value(currentValue);
                jWriter.name(MessageContract.Field.MSG_CONTENT_CONTEXT).value(currentVersion);
                jWriter.endObject();
            }
            jWriter.endArray();
            jWriter.close();
        } catch (IOException e) {
            Log.e(LOG_TAG, "Exception: IO Exception in JsonWriter.", e);
        } catch (JSONException e) {
            Log.e(LOG_TAG, "Exception: Improper Message Format.", e);
        }
        return sWriter.toString();
    }
}
