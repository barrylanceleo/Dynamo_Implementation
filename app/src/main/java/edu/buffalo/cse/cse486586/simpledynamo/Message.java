package edu.buffalo.cse.cse486586.simpledynamo;

public class Message {
    private static final String LOG_TAG = Message.class.getSimpleName();

    int type;
    int id;
    int sourceId;
    int coordinatorId;
    Content content;
    int responseFlag;

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
        this.content  = this.new Content(another.content);
        this.responseFlag  = another.responseFlag;
    }

    @Override
    public String toString() {
        return MessageContract.Field.MSG_FIELD_TYPE +": " + type + "\n" +
                MessageContract.Field.MSG_FIELD_ID +": " + id + "\n" +
                MessageContract.Field.MSG_FIELD_SOURCE_ID +": " + sourceId + "\n" +
                MessageContract.Field.MSG_FIELD_COORDINATOR_ID +": " + coordinatorId + "\n" +
                MessageContract.Field.MSG_FIELD_CONTENT +": " + content + "\n" +
                MessageContract.Field.MSG_FIELD_RESPONSE_FLAG +": " + responseFlag;
    }

    String getKey() {
        return content.key;
    }
    String getValue() {
        return content.value;
    }
    String getContext() {
        return content.context;
    }

    void setKey(String key) {
        content.key = key;
    }
    void setValue(String value) {
        content.value = value;
    }
    void setContext(String context) {
        content.context = context;
    }

    class Content {
        String key;
        String value;
        String context;

        Content(String key, String value, String context) {
            this.key = key;
            this.value = value;
            this.context = context;
        }

        Content(Content another) {
            this.key = another.key;
            this.value = another.value;
            this.context = another.context;
        }

        @Override
        public String toString() {
            return MessageContract.Field.MSG_CONTENT_KEY + ": " + key + "\n" +
                    MessageContract.Field.MSG_CONTENT_VALUE + ": " + value + "\n" +
                    MessageContract.Field.MSG_CONTENT_CONTEXT + ": " + context;
        }
    }
}
