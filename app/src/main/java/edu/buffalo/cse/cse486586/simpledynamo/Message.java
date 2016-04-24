package edu.buffalo.cse.cse486586.simpledynamo;

public class Message {

    private static final String LOG_TAG = Message.class.getSimpleName();
    public static final int RESPONSE_TIMEOUT = 500;
    public static int messageCounter = 0;

    int type;
    int id;
    int sourceId;
    int coordinatorId;
    String contents;
    int responseFlag;

    Message(int type, int id, int sourceId) {
        this.type = type;
        this.id = id;
        this.sourceId =sourceId;
    }
    public static class Type {
        final static int MSG_INSERT_INITIATE_REQUEST = 0;
        final static int MSG_INSERT_REQUEST = 1;
        final static int MSG_INSERT_RESPONSE = 2;
        final static int MSG_INSERT_COMPLETION_RESPONSE = 3;
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
        final static String MSG_FIELD_CONTENTS = "MSG_CONTENTS";
        final static String MSG_FIELD_RESPONSE_FLAG = "MSG_RESPONSE_FLAG";

        final static String MSG_CONTENT_KEY = "MSG_CONTENT_KEY";
        final static String MSG_CONTENT_VALUE = "MSG_CONTENT_VALUE";
        final static String MSG_CONTENT_CONTEXT = "MSG_CONTENT_CONTEXT";
    }

    @Override
    public String toString() {
        return Field.MSG_FIELD_TYPE +": " + type + "\n" +
                Field.MSG_FIELD_ID +": " + id + "\n" +
                Field.MSG_FIELD_SOURCE_ID +": " + sourceId + "\n" +
                Field.MSG_FIELD_COORDINATOR_ID +": " + coordinatorId + "\n" +
                Field.MSG_FIELD_CONTENTS +": " + contents + "\n" +
                Field.MSG_FIELD_RESPONSE_FLAG +": " + responseFlag;
    }
}
