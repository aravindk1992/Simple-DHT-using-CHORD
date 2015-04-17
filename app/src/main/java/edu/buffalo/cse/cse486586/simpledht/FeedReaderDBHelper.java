package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by arvindramesh on 3/31/15.
 */

// Reference http://developer.android.com/training/basics/data-storage/databases.html

public class FeedReaderDBHelper extends SQLiteOpenHelper {

    private static final String TEXT_TYPE = " TEXT";
    private static final String COMMA_SEP = ",";
    public static final String TABLE_NAME = "SimpleDHTDB";
    private static final int DATABASE_VERSION = 1;
    private static final String DATABASE_NAME = "SimpleDHT.db";

    private static final String SQL_CREATE_ENTRIES =
            "CREATE TABLE " +TABLE_NAME+ " (" + "key" + TEXT_TYPE + "UNIQUE "+COMMA_SEP + "value" + TEXT_TYPE+");";
    private static final String SQL_DELETE_ENTRIES =
            "DROP TABLE IF EXISTS " + TABLE_NAME;

    public FeedReaderDBHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL(SQL_CREATE_ENTRIES);

    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL(SQL_DELETE_ENTRIES);
        onCreate(db);
    }
}
