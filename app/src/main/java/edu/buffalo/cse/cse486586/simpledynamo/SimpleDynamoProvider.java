package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.util.Log;


public class SimpleDynamoProvider extends ContentProvider {

	private static final String LOG_TAG = SimpleDynamoProvider.class.getSimpleName();

	// The URI Matcher used by this content provider.
	private static final UriMatcher sUriMatcher = buildUriMatcher();

	private static DynamoDbHelper mDynamoDbHelper;
	// URI types
	static final int DYNAMO = 100;
	static final int NODE = 200;

    private static Coordinator mCoordinator;
	private static Thread backgroundThread;

	static UriMatcher buildUriMatcher() {
		final UriMatcher matcher = new UriMatcher(UriMatcher.NO_MATCH);
		final String authority = DatabaseContract.CONTENT_AUTHORITY;

		// For each type of URI you want to add, create a corresponding code.
		matcher.addURI(authority, null, DYNAMO);
		matcher.addURI(authority, DatabaseContract.PATH_NODE, NODE);
		return matcher;
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

		// Create a Database helper
		mDynamoDbHelper = new DynamoDbHelper(getContext());
		return true;
	}

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
		final SQLiteDatabase db = mDynamoDbHelper.getWritableDatabase();
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        Log.v(LOG_TAG, "INSERT: KEY: " + key + " VALUE: " + value);

		Uri returnUri;
		switch (sUriMatcher.match(uri)) {
			case NODE:
				// insert into this node
				db.beginTransaction();
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
				if(oldValue.getCount() == 0) {
					values.put(DatabaseContract.DynamoEntry.COLUMN_CONTEXT,
							Integer.toString(1));
				}
				else {
					oldValue.moveToFirst();
					String oldContext = oldValue.getString(oldValue.getColumnIndex(
							DatabaseContract.DynamoEntry.COLUMN_CONTEXT));
					values.put(DatabaseContract.DynamoEntry.COLUMN_CONTEXT,
							Integer.toString(Integer.parseInt(oldContext)+1));
				}

				// insert
				long _id;
				_id = db.insertWithOnConflict(DatabaseContract.DynamoEntry.TABLE_NAME, null,
						values, SQLiteDatabase.CONFLICT_REPLACE);
				db.setTransactionSuccessful();
				db.endTransaction();
				if ( _id > 0 ) {
					returnUri = DatabaseContract.DynamoEntry.DYNAMO_URI.buildUpon().appendPath(key).build();
				}
				else {
					throw new android.database.SQLException("Failed to insert row into " + uri);
				}
				break;
			case DYNAMO:
				// forward the request to the coordinator
				Message initiateMessage = new Message(MessageContract.Type.MSG_INSERT_INITIATE_REQUEST,
						MessageContract.messageCounter++, mCoordinator.MY_ID);
				initiateMessage.coordinatorId = 0;
				initiateMessage.content = initiateMessage.new Content(key, value, null);
				mCoordinator.mSender.sendMessage(Utility.convertMessageToJSON(initiateMessage),
						mCoordinator.findCoordinatingNode(key).port);
				// messageMap is used to keep track of the sent messaged
				mCoordinator.messageMap.put(initiateMessage.id, initiateMessage);

				synchronized (initiateMessage) {
					try {
						wait(MessageContract.RESPONSE_TIMEOUT);
						mCoordinator.messageMap.remove(initiateMessage.id);
					} catch (InterruptedException e)
					{
						Log.e(LOG_TAG, "Insert interrupted before receiving a response", e);
					}
					if(initiateMessage.responseFlag == MessageContract.ResponseFlag.MSG_RESPONSE_FLAG_OK) {
						Log.i(LOG_TAG, "Response Received: " +initiateMessage);
					}
					else {
						// TODO Handle Failure, Need to forward request to next node.
						Log.e(LOG_TAG, "Node Id: " +initiateMessage.coordinatorId +"has failed.\n" +
								"Need to forward request to next node.");
					}
				}
				returnUri = DatabaseContract.DynamoEntry.DYNAMO_URI.buildUpon().appendPath(key).build();
				break;
			default:
				throw new UnsupportedOperationException("Unknown uri: " + uri);
		}
		getContext().getContentResolver().notifyChange(uri, null);
		return returnUri;
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
