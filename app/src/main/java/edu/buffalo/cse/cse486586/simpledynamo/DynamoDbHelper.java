package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
  * Helper class for SQLite database
**/

public class DynamoDbHelper extends SQLiteOpenHelper {

    private static final String DATABASE_NAME = "dynamo.db";
    private static final int DATABASE_VERSION = 1;

    DynamoDbHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        final String SQL_CREATE_TABLE = "CREATE TABLE " +
                DatabaseContract.DynamoEntry.TABLE_NAME + " (" +
                DatabaseContract.DynamoEntry.COLUMN_KEY + " TEXT PRIMARY KEY, " +
                DatabaseContract.DynamoEntry.COLUMN_VALUE + " TEXT, " +
                DatabaseContract.DynamoEntry.COLUMN_OWNER_ID + " INTEGER, " +
                DatabaseContract.DynamoEntry.COLUMN_CONTEXT + " TEXT " +
                " );";
        db.execSQL(SQL_CREATE_TABLE);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("DROP TABLE IF EXISTS " + DatabaseContract.DynamoEntry.TABLE_NAME);
        onCreate(db);
    }
}
