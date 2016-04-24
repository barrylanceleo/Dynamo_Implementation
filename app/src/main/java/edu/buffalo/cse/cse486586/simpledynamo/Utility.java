package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created by barry on 4/23/16.
 */
public class Utility {
    private static final String LOG_TAG = Utility.class.getSimpleName();

    public static String buildMessageContent (String key, String value, String context) {
        JSONObject message = new JSONObject();
        try {
            message.put(Message.Field.MSG_FIELD_TYPE, key);
            message.put(Message.Field.MSG_FIELD_TYPE, value);
            message.put(Message.Field.MSG_FIELD_TYPE, context);
        } catch (JSONException e) {
            Log.e(LOG_TAG, "Unable build Message Content.", e);
        }
        return message.toString();
    }

    public static String convertMessageToJSON(Message messaage) {
        JSONObject jsonMessage = new JSONObject();
        try{
            jsonMessage.put(Message.Field.MSG_FIELD_TYPE, messaage.type);
            jsonMessage.put(Message.Field.MSG_FIELD_ID, messaage.id);
            jsonMessage.put(Message.Field.MSG_FIELD_SOURCE_ID, messaage.sourceId);
            jsonMessage.put(Message.Field.MSG_FIELD_COORDINATOR_ID, messaage.coordinatorId);
            jsonMessage.put(Message.Field.MSG_FIELD_CONTENTS, messaage.contents);
            jsonMessage.put(Message.Field.MSG_FIELD_RESPONSE_FLAG, messaage.responseFlag);
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
            message = new Message(messageJSON.getInt(Message.Field.MSG_FIELD_TYPE),
                    messageJSON.getInt(Message.Field.MSG_FIELD_ID),
                    messageJSON.getInt(Message.Field.MSG_FIELD_SOURCE_ID));
            message.coordinatorId = messageJSON.getInt(Message.Field.MSG_FIELD_COORDINATOR_ID);
            message.contents = messageJSON.getString(Message.Field.MSG_FIELD_CONTENTS);
            message.responseFlag = messageJSON.getInt(Message.Field.MSG_FIELD_RESPONSE_FLAG);
        } catch (JSONException e) {
            Log.e(LOG_TAG, "Exception: Improper Message Format.");
            return null;
        } catch (NumberFormatException e) {
            Log.e(LOG_TAG, "Exception: Improper Message Format.");
            return null;
        }
        return message;
    }
}
