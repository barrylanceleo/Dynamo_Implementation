package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by barry on 4/24/16.
 */
public class MessageContract {

    public static final int RESPONSE_TIMEOUT = 500;
    public static int messageCounter = 0;

    public static class Type {
        // message types for insert operation
        final static int MSG_INSERT_INITIATE_REQUEST = 100;
        final static int MSG_INSERT_REQUEST = 101;
        final static int MSG_INSERT_RESPONSE = 102;
        final static int MSG_INSERT_INITIATE_RESPONSE = 103;
    }

    public static class ResponseFlag {
        final static int MSG_RESPONSE_FLAG_NOTHING = 0;
        final static int MSG_RESPONSE_FLAG_OK = 1;
    }

    public static class Field {
        final static String MSG_FIELD_TYPE = "MSG_TYPE";
        final static String MSG_FIELD_ID = "MSG_ID";
        final static String MSG_FIELD_SOURCE_ID = "MSG_SOURCE_ID";
        final static String MSG_FIELD_COORDINATOR_ID = "MSG_COORDINATOR_ID";
        final static String MSG_FIELD_CONTENT = "MSG_CONTENT";
        final static String MSG_FIELD_RESPONSE_FLAG = "MSG_RESPONSE_FLAG";

        final static String MSG_CONTENT_KEY = "MSG_CONTENT_KEY";
        final static String MSG_CONTENT_VALUE = "MSG_CONTENT_VALUE";
        final static String MSG_CONTENT_CONTEXT = "MSG_CONTENT_CONTEXT";
    }
}
