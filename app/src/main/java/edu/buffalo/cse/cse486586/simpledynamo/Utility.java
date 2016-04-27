package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.util.JsonWriter;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

public class Utility {
    private static final String LOG_TAG = Utility.class.getSimpleName();

    public static String buildkeyValueContent (String key, String value, String context) {
        JSONObject message = new JSONObject();
        try {
            message.put(MessageContract.Field.MSG_CONTENT_KEY, key);
            message.put(MessageContract.Field.MSG_CONTENT_VALUE, value);
            message.put(MessageContract.Field.MSG_CONTENT_CONTEXT, context);
        } catch (JSONException e) {
            Log.e(LOG_TAG, "Unable build Message Content.", e);
        }
        return message.toString();
    }

    public static String convertMessageToJSON(Message messaage) {
        JSONObject jsonMessage = new JSONObject();
        try{
            jsonMessage.put(MessageContract.Field.MSG_FIELD_TYPE, messaage.type);
            jsonMessage.put(MessageContract.Field.MSG_FIELD_ID, messaage.id);
            jsonMessage.put(MessageContract.Field.MSG_FIELD_SOURCE_ID, messaage.sourceId);
            jsonMessage.put(MessageContract.Field.MSG_FIELD_COORDINATOR_ID, messaage.coordinatorId);
            jsonMessage.put(MessageContract.Field.MSG_FIELD_CONTENT, messaage.content);
        } catch (JSONException e) {
            Log.e(LOG_TAG, "Unable convert Message to JSON", e);
        }
        return jsonMessage.toString();
    }

    public static Message buildMessageObjectFromJson(String messageString) {
        JSONObject messageJSON;
        Message message;
        try {
            messageJSON = new JSONObject(messageString);
            message = new Message(messageJSON.getInt(MessageContract.Field.MSG_FIELD_TYPE),
                    messageJSON.getInt(MessageContract.Field.MSG_FIELD_ID),
                    messageJSON.getInt(MessageContract.Field.MSG_FIELD_SOURCE_ID));
            message.coordinatorId = messageJSON.getInt(MessageContract.Field.MSG_FIELD_COORDINATOR_ID);
            message.content = messageJSON.getString(MessageContract.Field.MSG_FIELD_CONTENT);
        } catch (JSONException e) {
            Log.e(LOG_TAG, "Exception: Improper Message Format.");
            return null;
        } catch (NumberFormatException e) {
            Log.e(LOG_TAG, "Exception: Improper Message Format.");
            return null;
        }
        return message;
    }

    public static int getTypeFromMessageJson(String messageString) {
        try {
            JSONObject messageJSON = new JSONObject(messageString);
            return messageJSON.getInt(MessageContract.Field.MSG_FIELD_TYPE);
        } catch (JSONException e) {
            Log.e(LOG_TAG, "Exception: Improper Message Format.");
            return 0;
        } catch (NumberFormatException e) {
            Log.e(LOG_TAG, "Exception: Improper Message Format.");
            return 0;
        }
    }

    public static Cursor convertResponseToCursor(String queryResponse){
        MatrixCursor cursor = new MatrixCursor(new String[]{DatabaseContract.DynamoEntry.COLUMN_KEY,
                DatabaseContract.DynamoEntry.COLUMN_VALUE, DatabaseContract.DynamoEntry.COLUMN_CONTEXT});
        try {
            JSONArray messageJSON = new JSONArray(queryResponse);
            for(int i = 0; i < messageJSON.length(); i++){
                JSONObject jObject = messageJSON.getJSONObject(i);
                String key = jObject.getString(MessageContract.Field.MSG_CONTENT_KEY);
                String value = jObject.getString(MessageContract.Field.MSG_CONTENT_VALUE);
                String context = jObject.getString(MessageContract.Field.MSG_CONTENT_CONTEXT);
                cursor.addRow(new String[]{key, value, context});
            }
        } catch (JSONException e) {
            Log.e(LOG_TAG, "Exception: Improper Message Format.", e);
        }
        cursor.moveToFirst();
        return cursor;
    }

    public static ContentValues[] convertResponseToCvArray(String queryResponse){
        ContentValues [] cvs;
        try {
            JSONArray messageJSON = new JSONArray(queryResponse);
            cvs = new ContentValues[messageJSON.length()];
            for(int i = 0; i < messageJSON.length(); i++){
                ContentValues cv = new ContentValues();
                JSONObject jObject = messageJSON.getJSONObject(i);
                cv.put(DatabaseContract.DynamoEntry.COLUMN_KEY,
                        jObject.getString(MessageContract.Field.MSG_CONTENT_KEY));
                cv.put(DatabaseContract.DynamoEntry.COLUMN_VALUE,
                        jObject.getString(MessageContract.Field.MSG_CONTENT_VALUE));
                cv.put(DatabaseContract.DynamoEntry.COLUMN_CONTEXT,
                        jObject.getString(MessageContract.Field.MSG_CONTENT_CONTEXT));
                cvs[i] = cv;
            }
            return cvs;
        } catch (JSONException e) {
            Log.e(LOG_TAG, "Exception: Improper Message Format.", e);
            return new ContentValues[]{};
        }
    }

    public static String convertCursorToString(Cursor cursor) {
        cursor.moveToFirst();
        StringWriter sWriter = new StringWriter();
        JsonWriter jWriter = new JsonWriter(sWriter);
        try {
            jWriter.beginArray();
            while(!cursor.isAfterLast())
            {
                String key = cursor.getString(cursor.getColumnIndex(DatabaseContract.DynamoEntry.COLUMN_KEY));
                String value = cursor.getString(cursor.getColumnIndex(DatabaseContract.DynamoEntry.COLUMN_VALUE));
                String context = cursor.getString(cursor.getColumnIndex(DatabaseContract.DynamoEntry.COLUMN_CONTEXT));
                jWriter.beginObject();
                jWriter.name(MessageContract.Field.MSG_CONTENT_KEY).value(key);
                jWriter.name(MessageContract.Field.MSG_CONTENT_VALUE).value(value);
                jWriter.name(MessageContract.Field.MSG_CONTENT_CONTEXT).value(context);
                jWriter.endObject();
                cursor.moveToNext();
            }
            jWriter.endArray();
            jWriter.close();
            cursor.moveToFirst();
        } catch (IOException e) {
            Log.e(LOG_TAG, "Exception: IO Exception in JsonWriter.", e);
        }
        return sWriter.toString();
    }

    public static String nodeListToString(List<Coordinator.Node> nodes) {
        StringWriter sWriter = new StringWriter();
        JsonWriter jWriter = new JsonWriter(sWriter);
        try {
            jWriter.beginArray();
            for(Coordinator.Node node : nodes) {
                jWriter.value(node.id);
            }
            jWriter.endArray();
            jWriter.close();
        } catch (IOException e) {
            Log.e(LOG_TAG, "Exception: IO Exception in JsonWriter.", e);
        }
        return sWriter.toString();
    }

    public static String [] stringToNodeIdList(String nodeIds) {
        String [] nodeIdArray;
        try {
            JSONArray nodeIdJSON = new JSONArray(nodeIds);
            nodeIdArray = new String[nodeIdJSON.length()];
            for(int i = 0; i < nodeIdJSON.length(); i++){
                nodeIdArray[i] = Integer.toString(nodeIdJSON.getInt(i));
            }
            return nodeIdArray;
        } catch (JSONException e) {
            Log.e(LOG_TAG, "Exception: Improper Message Format.", e);
            return new String [] {};
        }
    }

    public static ContentValues [] convertCursorToCvArray(Cursor cursor) {
        cursor.moveToFirst();
        ContentValues [] cvs = new ContentValues[cursor.getCount()];
        while(!cursor.isAfterLast())
        {
            String key = cursor.getString(cursor.getColumnIndex(DatabaseContract.DynamoEntry.COLUMN_KEY));
            String value = cursor.getString(cursor.getColumnIndex(DatabaseContract.DynamoEntry.COLUMN_VALUE));
            String context = cursor.getString(cursor.getColumnIndex(DatabaseContract.DynamoEntry.COLUMN_CONTEXT));
            ContentValues cv = new ContentValues();
            cv.put(DatabaseContract.DynamoEntry.COLUMN_KEY, key);
            cv.put(DatabaseContract.DynamoEntry.COLUMN_VALUE, value);
            cv.put(DatabaseContract.DynamoEntry.COLUMN_CONTEXT, context);
            cursor.moveToNext();
        }
        cursor.moveToFirst();
        return cvs;
    }

    public static void printNodeList(List<Coordinator.Node> nodes)
    {
        for(Coordinator.Node node : nodes) {
            Log.i(LOG_TAG, node.toString());
        }
    }
}
