package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.util.Log;

import java.util.List;


public class SimpleDynamoProvider extends ContentProvider {

    private static final String LOG_TAG = SimpleDynamoProvider.class.getSimpleName();

    // The URI Matcher used by this content provider.
    private static final UriMatcher sUriMatcher = buildUriMatcher();

    private static DynamoDbHelper mDynamoDbHelper;
    // URI types
    static final int DYNAMO = 100;
    static final int NODE = 200;
    static final int BLIND = 300;


    private static Coordinator mCoordinator;
    private static Thread backgroundThread;

    static UriMatcher buildUriMatcher() {
        final UriMatcher matcher = new UriMatcher(UriMatcher.NO_MATCH);
        final String authority = DatabaseContract.CONTENT_AUTHORITY;

        // For each type of URI you want to add, create a corresponding code.
        matcher.addURI(authority, null, DYNAMO);
        matcher.addURI(authority, DatabaseContract.PATH_NODE, NODE);
        matcher.addURI(authority, DatabaseContract.PATH_BLIND, BLIND);

        return matcher;
    }

    @Override
    public boolean onCreate() {
        Log.i(LOG_TAG, "Creating the Content Provider.");
        // Create a Database helper
        mDynamoDbHelper = new DynamoDbHelper(getContext());
        // create the coordinator
        if (mCoordinator == null) {
            Log.i(LOG_TAG, "Creating Coordinator.");
            mCoordinator = Coordinator.getInstance(getContext());
            mCoordinator.isResynced = false;
            // start handling requests in a background thread,
            // this will process only re-sync responses until re-sync is completed
            backgroundThread = new Thread(new Runnable() {
                public void run() {
                    mCoordinator.start();
                }
            });
            backgroundThread.start();
            // re-sync
            mCoordinator.resync(this);
        }
        Log.i(LOG_TAG, "Node started and re-synced.");
        return true;
    }

    @Override
    public String getType(Uri uri) {
        // Use the Uri Matcher to determine what kind of URI this is.
        switch (sUriMatcher.match(uri)) {
            case DYNAMO:
                return DatabaseContract.DynamoEntry.CONTENT_TYPE;
            case NODE:
                return DatabaseContract.DynamoEntry.CONTENT_TYPE;
            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        SQLiteDatabase db;
        String key = values.getAsString(DatabaseContract.DynamoEntry.COLUMN_KEY);
        String value = values.getAsString(DatabaseContract.DynamoEntry.COLUMN_VALUE);
        int owner_id = mCoordinator.findCoordinatingNode(key).id;
        values.put(DatabaseContract.DynamoEntry.COLUMN_OWNER_ID, owner_id);
        Log.i(LOG_TAG, "INSERT: KEY: " + key + " VALUE: " + value
                +"\nHASH: " +Coordinator.genHash(key) +" OWNER: " + owner_id
                +"\nURI: " +uri.toString());
        Uri returnUri;
        switch (sUriMatcher.match(uri)) {
            case NODE:
                // insert into this node
                db = mDynamoDbHelper.getWritableDatabase();
                db.beginTransaction();
                Log.i(LOG_TAG, "Querying DB.");
                Cursor oldValue = db.query(
                        DatabaseContract.DynamoEntry.TABLE_NAME,
                        null,
                        "key=?",
                        new String[]{key},
                        null,
                        null,
                        null
                );
                Log.i(LOG_TAG, "Query on DB Complete.");
                // if key not present already insert context = 1, else ++context
                if (oldValue.getCount() == 0) {
                    values.put(DatabaseContract.DynamoEntry.COLUMN_CONTEXT,
                            Integer.toString(1));
                } else {
                    oldValue.moveToFirst();
                    String oldContext = oldValue.getString(oldValue.getColumnIndex(
                            DatabaseContract.DynamoEntry.COLUMN_CONTEXT));
                    values.put(DatabaseContract.DynamoEntry.COLUMN_CONTEXT,
                            Integer.toString(Integer.parseInt(oldContext) + 1));
                }
                oldValue.close();

                // insert
                long _id;
                _id = db.insertWithOnConflict(DatabaseContract.DynamoEntry.TABLE_NAME, null,
                        values, SQLiteDatabase.CONFLICT_REPLACE);
                db.setTransactionSuccessful();
                db.endTransaction();
                if (_id > 0) {
                    returnUri = DatabaseContract.DynamoEntry.DYNAMO_URI.buildUpon().appendPath(key).build();
                } else {
                    throw new android.database.SQLException("Failed to insert row into " + uri);
                }
                Log.i(LOG_TAG, "Insert into DB: " +values.toString());
                break;
            case DYNAMO:
                // forward the request to the coordinator
                Message initiateMessage = new Message(MessageContract.Type.MSG_INSERT_INITIATE_REQUEST,
                        MessageContract.messageCounter.getAndIncrement(), mCoordinator.MY_ID);
                initiateMessage.coordinatorId = 0;
                initiateMessage.content = Utility.buildkeyValueContent(key, value, "");
                // send and wait for response
                sendAndWaitForResponse(initiateMessage, mCoordinator.getPreferenceList(key));
                returnUri = DatabaseContract.DynamoEntry.DYNAMO_URI.buildUpon().appendPath(key).build();
                break;
            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }
        return returnUri;
    }

    @Override
    public int bulkInsert(Uri uri, ContentValues[] values) {
        SQLiteDatabase db;
        switch (sUriMatcher.match(uri)) {
            case BLIND:
                return blindBulkInsert(uri, values);
            case NODE:
                int insertCount = 0;
                db = mDynamoDbHelper.getWritableDatabase();
                db.beginTransaction();
                for (ContentValues cv : values) {
                    String key = cv.getAsString(DatabaseContract.DynamoEntry.COLUMN_KEY);
                    int owner_id = mCoordinator.findCoordinatingNode(key).id;
                    cv.put(DatabaseContract.DynamoEntry.COLUMN_OWNER_ID, owner_id);
                    Cursor oldValue = db.query(
                            DatabaseContract.DynamoEntry.TABLE_NAME,
                            null,
                            "key=?",
                            new String[]{key},
                            null,
                            null,
                            null
                    );
                    // if key not present already insert context = 1, else ++context
                    if (oldValue.getCount() == 0) {
                        cv.put(DatabaseContract.DynamoEntry.COLUMN_CONTEXT,
                                Integer.toString(1));
                    } else {
                        oldValue.moveToFirst();
                        String oldContext = oldValue.getString(oldValue.getColumnIndex(
                                DatabaseContract.DynamoEntry.COLUMN_CONTEXT));
                        cv.put(DatabaseContract.DynamoEntry.COLUMN_CONTEXT,
                                Integer.toString(Integer.parseInt(oldContext) + 1));
                    }
                    oldValue.close();

                    // insert
                    long _id;
                    _id = db.insertWithOnConflict(DatabaseContract.DynamoEntry.TABLE_NAME, null,
                            cv, SQLiteDatabase.CONFLICT_REPLACE);
                    if (_id > 0) {
                        insertCount++;
                    }
                }
                db.setTransactionSuccessful();
                db.endTransaction();
                Log.i(LOG_TAG, "Bulk Insert into DB: " +insertCount +" rows");
                return insertCount;
            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }
    }

    // used for re-synchronization, uses the context and coordinator id present in the Cv
    public int blindBulkInsert(Uri uri, ContentValues[] values) {
        SQLiteDatabase db;
        switch (sUriMatcher.match(uri)) {
            case BLIND:
                int insertedCount = 0;
                db = mDynamoDbHelper.getWritableDatabase();
                db.beginTransaction();
                for (ContentValues cv : values) {
                    String key = cv.getAsString(DatabaseContract.DynamoEntry.COLUMN_KEY);
                    int owner_id = mCoordinator.findCoordinatingNode(key).id;
                    cv.put(DatabaseContract.DynamoEntry.COLUMN_OWNER_ID, owner_id);
                    long _id;
                    _id = db.insertWithOnConflict(DatabaseContract.DynamoEntry.TABLE_NAME, null,
                            cv, SQLiteDatabase.CONFLICT_REPLACE);
                    if (_id > 0) {
                        insertedCount++;
                    }
                }
                db.setTransactionSuccessful();
                db.endTransaction();
                Log.i(LOG_TAG, "Blind Bulk Insert into DB: " +insertedCount +" rows");
                return insertedCount;
            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        Log.i(LOG_TAG, "QUERY: selection: " + selection +"\nURI: " +uri.toString());
        SQLiteDatabase db;
        Cursor returnCursor;
        switch (sUriMatcher.match(uri)) {
            case NODE:
                db = mDynamoDbHelper.getReadableDatabase();
                returnCursor = db.query(
                        DatabaseContract.DynamoEntry.TABLE_NAME,
                        projection,    // The columns to return from the query
                        selection,     // The columns for the where clause
                        selectionArgs, // The values for the where clause
                        null,          // don't group the rows
                        null,          // don't filter by row groups
                        sortOrder        // The sort order
                );
                Log.i(LOG_TAG, "Query Result: " +Utility.dumpCursor(returnCursor));
                return returnCursor;
            case DYNAMO:
                String key = selection;
                if (selection.equals("*")) {
                    // forward the request to myself
                    Message initiateMessage = new Message(MessageContract.Type.MSG_QUERY_INITIATE_REQUEST,
                            MessageContract.messageCounter.getAndIncrement(), mCoordinator.MY_ID);
                    initiateMessage.coordinatorId = 0;
                    initiateMessage.content = key;
                    // send and wait for response
                    Message response = sendAndWaitForResponse(initiateMessage, mCoordinator.getPreferenceList(key));
                    // convert response to cursor
                    if(response != null) {
                        returnCursor = Utility.convertResponseToCursor(response.content);
                        Log.i(LOG_TAG, "Query Result: " +Utility.dumpCursor(returnCursor));
                        return returnCursor;
                    }
                    return null;
                } else if (selection.equals("@")) {
                    // just return all the entries in my node
                    projection = new String[] {DatabaseContract.DynamoEntry.COLUMN_KEY,
                            DatabaseContract.DynamoEntry.COLUMN_VALUE};
                    selection = DatabaseContract.DynamoEntry.COLUMN_VALUE +" IS NOT NULL AND " +
                            DatabaseContract.DynamoEntry.COLUMN_VALUE +" != ?";
                    selectionArgs = new String[]{"null"};
                    return query(DatabaseContract.DynamoEntry.NODE_URI,
                            projection, selection, selectionArgs, sortOrder);
                } else {
                    // forward the request to the coordinator
                    Message initiateMessage = new Message(MessageContract.Type.MSG_QUERY_INITIATE_REQUEST,
                            MessageContract.messageCounter.getAndIncrement(), mCoordinator.MY_ID);
                    initiateMessage.coordinatorId = 0;
                    initiateMessage.content = key;
                    // send and wait for response
                    Message response = sendAndWaitForResponse(
                            initiateMessage, mCoordinator.getPreferenceList(key));
                    // convert response to cursor
                    returnCursor = Utility.convertResponseToCursor(response.content);
                    Log.i(LOG_TAG, "Query Result: " +Utility.dumpCursor(returnCursor));
                    return returnCursor;
                }
            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.i(LOG_TAG, "DELETE: selection: " + selection +"\nURI: " +uri.toString());
        switch (sUriMatcher.match(uri)) {
            case NODE:
                if (selection == null) {
                    // delete all the entries in my node
                    return deleteAll(DatabaseContract.DynamoEntry.NODE_URI);
                } else {
                    // delete is insert with null value
                    ContentValues deleteCv = new ContentValues();
                    deleteCv.put(DatabaseContract.DynamoEntry.COLUMN_KEY, selectionArgs[0]);
                    deleteCv.put(DatabaseContract.DynamoEntry.COLUMN_VALUE, (String) null);
                    insert(DatabaseContract.DynamoEntry.NODE_URI, deleteCv);
                    return 1;
                }
            case DYNAMO:
                String key = selection;
                if (key.equals("*")) {
                    // forward the request to myself
                    Message initiateMessage = new Message(MessageContract.Type.MSG_DELETE_INITIATE_REQUEST,
                            MessageContract.messageCounter.getAndIncrement(), mCoordinator.MY_ID);
                    initiateMessage.coordinatorId = 0;
                    initiateMessage.content = key;
                    // send and wait for response
                    Message response = sendAndWaitForResponse(
                            initiateMessage, mCoordinator.getPreferenceList(key));
                    // convert response to count
                    return Integer.parseInt(response.content);
                } else if (key.equals("@")) {
                    // just delete all the entries in my node
                    selection = null;
                    selectionArgs = null;
                    return delete(DatabaseContract.DynamoEntry.NODE_URI,
                            selection, selectionArgs);
                } else {
                    // forward the request to the coordinator
                    Message initiateMessage = new Message(MessageContract.Type.MSG_DELETE_INITIATE_REQUEST,
                            MessageContract.messageCounter.getAndIncrement(), mCoordinator.MY_ID);
                    initiateMessage.coordinatorId = 0;
                    initiateMessage.content = key;
                    // send and wait for response
                    Message response = sendAndWaitForResponse(
                            initiateMessage, mCoordinator.getPreferenceList(key));
                    // convert response to count
                    return Integer.parseInt(response.content);
                }
            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }
    }

    private int deleteAll(Uri uri) {
        switch (sUriMatcher.match(uri)) {
            case NODE:
                // query all the values and update their values with null
                Cursor queryCursor = query(DatabaseContract.DynamoEntry.NODE_URI, null, null, null, null);
                ContentValues[] queryCvs = Utility.convertCursorToCvArray(queryCursor);
                queryCursor.close();
                return bulkInsert(DatabaseContract.DynamoEntry.NODE_URI, queryCvs);
            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    Message sendAndWaitForResponse(Message message, List<Coordinator.Node> nodesToTry) {
        for (Coordinator.Node node : nodesToTry) {
            long startTime = System.currentTimeMillis();
            mCoordinator.mSender.sendMessage(Utility.convertMessageToJSON(message), node.port);
            Log.i(LOG_TAG, "Sent Message to node " + node.id + ": " + message);
            // messageMap is used to keep track of the sent messages
            mCoordinator.messageMap.put(message.id, message);
            int sendType = message.type;
            Message response;
            synchronized (message) {
                try {
                    //message.wait(MessageContract.LONG_TIMEOUT);
                    message.wait(MessageContract.INITIATE_REQUEST_TIMEOUT);
                    response = (Message) mCoordinator.messageMap.remove(message.id);
                    if (sendType != response.type) {
                        Log.i(LOG_TAG, "Response Received: " + response);
                        long endTime = System.currentTimeMillis();
                        Log.i(LOG_TAG, "TIME TAKEN FOR INITIATE RESPONSE: "+(endTime - startTime) +"ms.");
                        return response;
                    } else {
                        long endTime = System.currentTimeMillis();
                        Log.e(LOG_TAG, "Node Id: " + node.id + " has TIMED-OUT. Forwarding request to next node."
                                +"\nTIME TAKEN: " +(endTime - startTime) +"ms.");

                        //increment the message id so that, after time-out response to old messages are ignored
                        message.id = MessageContract.messageCounter.getAndIncrement();
                    }
                } catch (InterruptedException e) {
                    Log.e(LOG_TAG, "Request interrupted before receiving a response", e);
                }
            }
        }
        Log.e(LOG_TAG, "Did not receive response from any node in sendAndWaitForResponse(). This is bad.");
        return null;
    }
}
