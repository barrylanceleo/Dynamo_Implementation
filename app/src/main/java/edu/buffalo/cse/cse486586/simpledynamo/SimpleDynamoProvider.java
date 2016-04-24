package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.util.Log;


public class SimpleDynamoProvider extends ContentProvider {

	private static final String LOG_TAG = SimpleDynamoProvider.class.getSimpleName();
    private Coordinator mCoordinator;
	private Thread backgroundThread;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {

        String key = values.getAsString("key");
        String value = values.getAsString("value");
        Log.v(LOG_TAG, "INSERT INITIATION: KEY: " + key + " VALUE: " + value);

        // find the coordinator

        // forward the request to the coordinator
        Message initiateMessage = new Message(Message.Type.MSG_INSERT_INITIATE_REQUEST,
                                                Message.messageCounter++, mCoordinator.MY_ID);

        initiateMessage.coordinatorId = 0;
        initiateMessage.contents = Utility.buildMessageContent(key, value, null);
        mCoordinator.mSender.sendMessage(Utility.convertMessageToJSON(initiateMessage),
                mCoordinator.findCoordinatingNode(key).port);
        mCoordinator.messageMap.put(initiateMessage.id, initiateMessage);

        synchronized (initiateMessage) {
            try {
                wait(Message.RESPONSE_TIMEOUT);
            } catch (InterruptedException e)
            {
                Log.e(LOG_TAG, "Insert interrupted before receiving a response", e);
            }
            if(initiateMessage.responseFlag == Message.ResponseFlag.MSG_RESPONSE_FLAG_OK) {
                Log.i(LOG_TAG, "Response Received: " +initiateMessage);
            }
            else {
                Log.e(LOG_TAG, "Node Id: " +initiateMessage.coordinatorId +"has failed.\n" +
                        "Need to forward request to next node.");
            }
        }

        return null;
	}

	@Override
	public boolean onCreate() {
        Log.i(LOG_TAG, "Creating the Content Provider.");
        // start everything in a background thread
		if(backgroundThread == null) {
			backgroundThread = new Thread(new Runnable() {
				public void run()
				{
					mCoordinator = Coordinator.getInstance(getContext());
                    mCoordinator.start();
				}});
			backgroundThread.start();
            Log.i(LOG_TAG, "Started Coordinator in a new thread.");
		}
		return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}
}
