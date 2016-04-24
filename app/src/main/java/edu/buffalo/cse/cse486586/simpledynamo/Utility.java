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

            // message content
            JSONObject jsonContent = new JSONObject();
            jsonContent.put(MessageContract.Field.MSG_CONTENT_KEY, messaage.content.key);
            jsonContent.put(MessageContract.Field.MSG_CONTENT_VALUE, messaage.content.value);
            jsonContent.put(MessageContract.Field.MSG_CONTENT_CONTEXT, messaage.content.context);
            jsonMessage.put(MessageContract.Field.MSG_FIELD_CONTENT, jsonContent);

            jsonMessage.put(MessageContract.Field.MSG_FIELD_RESPONSE_FLAG, messaage.responseFlag);
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

            // message content
            JSONObject contentJSON = new JSONObject(
                    messageJSON.getString(MessageContract.Field.MSG_FIELD_CONTENT));
            message.content = message.new Content(
                    contentJSON.getString(MessageContract.Field.MSG_CONTENT_KEY),
                    contentJSON.getString(MessageContract.Field.MSG_CONTENT_VALUE),
                    contentJSON.getString(MessageContract.Field.MSG_CONTENT_CONTEXT));

            message.responseFlag = messageJSON.getInt(MessageContract.Field.MSG_FIELD_RESPONSE_FLAG);
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
