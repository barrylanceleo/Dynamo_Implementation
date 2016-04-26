package edu.buffalo.cse.cse486586.simpledynamo;

public class MessageContract {

    public static final int RESPONSE_TIMEOUT = 500;
    public static int messageCounter = 0;

    public static class Type {
        // types for insert operation
        final static int MSG_INSERT_INITIATE_REQUEST = 100;
        final static int MSG_INSERT_REQUEST = 101;
        final static int MSG_INSERT_RESPONSE = 102;
        final static int MSG_INSERT_INITIATE_RESPONSE = 103;

        // types for query operation
        final static int MSG_QUERY_INITIATE_REQUEST =200;
        final static int MSG_QUERY_REQUEST = 201;
        final static int MSG_QUERY_RESPONSE = 202;
        final static int MSG_QUERY_INITIATE_RESPONSE = 203;

        // types for delete operation
        final static int MSG_DELETE_INITIATE_REQUEST =300;
        final static int MSG_DELETE_REQUEST = 301;
        final static int MSG_DELETE_RESPONSE = 302;
        final static int MSG_DELETE_INITIATE_RESPONSE = 303;

        // types for sync operation
        final static int MSG_SYNC_REQUEST = 400;
        final static int MSG_SYNC_RESPONSE = 401;
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
