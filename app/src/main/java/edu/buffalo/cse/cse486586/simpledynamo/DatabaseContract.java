package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.net.Uri;

public class DatabaseContract {
    public static final String CONTENT_AUTHORITY = "edu.buffalo.cse.cse486586.simpledynamo.provider";
    public static final Uri BASE_CONTENT_URI = Uri.parse("content://" + CONTENT_AUTHORITY);

    // path to access node specific data
    public static final String PATH_NODE = "node";
    public static final String PATH_BLIND = "blind";

    // defines the dynamo table
    public static final class DynamoEntry {
        public static final Uri DYNAMO_URI = BASE_CONTENT_URI;
        public static final Uri NODE_URI = BASE_CONTENT_URI.buildUpon().appendPath(PATH_NODE).build();
        public static final Uri BLIND_URI = BASE_CONTENT_URI.buildUpon().appendPath(PATH_BLIND).build();

        public static final String CONTENT_TYPE =
                ContentResolver.CURSOR_DIR_BASE_TYPE + "/" + CONTENT_AUTHORITY;
        public static final String CONTENT_ITEM_TYPE =
                ContentResolver.CURSOR_ITEM_BASE_TYPE + "/" + CONTENT_AUTHORITY;

        // Table name
        public static final String TABLE_NAME = "DynamoTable";

        //columns
        public static final String COLUMN_KEY = "key";
        public static final String COLUMN_VALUE = "value";
        public static final String COLUMN_CONTEXT = "context";
        public static final String COLUMN_OWNER_ID = "owner_id";
    }


}
