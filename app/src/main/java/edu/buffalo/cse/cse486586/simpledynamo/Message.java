package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Format for messages exchanged between nodes
**/


public class Message {
    private static final String LOG_TAG = Message.class.getSimpleName();

    int type;
    int id;
    int sourceId;
    int coordinatorId;
    String content;

    Message(int type, int id, int sourceId) {
        this.type = type;
        this.id = id;
        this.sourceId = sourceId;
    }

    Message(Message another) {
        this.type = another.type;
        this.id  = another.id;
        this.sourceId  = another.sourceId;
        this.coordinatorId  = another.coordinatorId;
        this.content  = another.content;
    }

    @Override
    public String toString() {
        return MessageContract.Field.MSG_FIELD_TYPE +": " + type + "\n" +
                MessageContract.Field.MSG_FIELD_ID +": " + id + "\n" +
                MessageContract.Field.MSG_FIELD_SOURCE_ID +": " + sourceId + "\n" +
                MessageContract.Field.MSG_FIELD_COORDINATOR_ID +": " + coordinatorId + "\n" +
                MessageContract.Field.MSG_FIELD_CONTENT +": " + content;
    }
}

