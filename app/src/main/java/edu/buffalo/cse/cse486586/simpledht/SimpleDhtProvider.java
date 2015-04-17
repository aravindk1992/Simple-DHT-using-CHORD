package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {
    FeedReaderDBHelper mDbHelper;
    ArrayList<String> list = new ArrayList<String>();
    ArrayList<String> lst = new ArrayList<String>();
    ArrayList<String> ports = new ArrayList<String>();
    ArrayList<String> NodeList ;
    ArrayList<String> globalList ;
    ArrayList<String> route;
    HashMap<String,String> contents;
    HashMap<String,String> temp;
    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    int SERVER_PORT = 10000;
    String nodeID = null;
    String a =null;
    String localAVD=null;
    int count =-1;
    String myPort=null;
    String portStr=null;
    String pred=null;
    String succ = null;
    String HashIt =null;
    String key=null;
    String value =null;
    int me=-1;
    int myself;
    MatrixCursor m;
    String forwardPort=null;
    StringBuilder Message = new StringBuilder();
    ContentResolver mContentResolver;
    StringBuilder QueryArgs = new StringBuilder();
    ContentValues vals= new ContentValues();
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        SQLiteDatabase db = mDbHelper.getWritableDatabase();

        if(pred==(null) && succ ==(null)){

            if (selection.equals("\"@\"") || selection.equals("\"*\"")) {
                db.delete(mDbHelper.TABLE_NAME,null,null);
                contents= new HashMap<String,String>();
            }
            else{
                db.delete(mDbHelper.TABLE_NAME, "key=" + "'" + selection + "'", null);
                contents.remove(selection);
            }
        }
        if(count>0){
            if(selection.equals("\"@\"")){
                db.delete(mDbHelper.TABLE_NAME,null,null);
                contents= new HashMap<String,String>();
            }
            else if(selection.equals("\"*\"")){
                db.delete(mDbHelper.TABLE_NAME,null,null);
                contents= new HashMap<String,String>();
                route = new ArrayList<String>();
                for(int i=0;i<=globalList.size()-1;i++){
                    if(globalList.get(i)!=portStr){
                        route.add(String.valueOf(Integer.parseInt(globalList.get(i))*2));
                    }
                }
                BmulticastMessage("DELETE ALL"+"\t"+myPort,route);
            }
            else{
                if(contents.containsKey(selection)){    // if key in local
                    // return it
                    db.delete(mDbHelper.TABLE_NAME, "key=" + "'" + selection + "'", null);
                    contents.remove(selection);
                }
                else{
                    // send to next

                    QueryArgs.append("DELETE_SELECTION"+"\t"+ selection);
                    QueryArgs.append("\t"+myPort+",");
                    route = new ArrayList<String>();
                    for(int i=0;i<=globalList.size()-1;i++){
                        route.add(String.valueOf(Integer.parseInt(globalList.get(i))*2));
                    }
                    BmulticastMessage(QueryArgs.toString(), route);
                    QueryArgs.delete(0,QueryArgs.length());

                }

            }
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        SQLiteDatabase db = mDbHelper.getWritableDatabase();
        key = (String) values.get("key");
        value =(String) values.get("value");

        try {
            HashIt = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

       if(pred==(null) && succ==(null)) {   // Only one avd hence local insert

            db.insertWithOnConflict(mDbHelper.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
            contents.put(key,value);
        }
       else if(pred.equals(succ)&& succ.equals(nodeID)&& nodeID.equals(pred)&& portStr.equals("5554")){
           db.insertWithOnConflict(mDbHelper.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
           contents.put(key,value);
       }
           else if (pred!=null && succ !=null && pred.equals(succ)) // Two AVDs
            {
                if (succ.compareTo(nodeID) > 0) {      // Case where Successor is Greater than Current Node
                    if (nodeID.compareTo(HashIt) < 0 && succ.compareTo(HashIt) > 0) {
                        System.out.println("FORWARD TO NEXT 1");
                        route = new ArrayList<String>();
                        if (globalList.size() > 1) {
                            me = globalList.indexOf(portStr);
                            if (me == 0) {
                                forwardPort = String.valueOf(Integer.parseInt(globalList.get(me + 1)) * 2);

                            } else if (me == globalList.size() - 1) {
                                forwardPort = String.valueOf(Integer.parseInt(globalList.get(0)) * 2);

                            } else {
                                forwardPort = String.valueOf(Integer.parseInt(globalList.get(me + 1)) * 2);

                            }

                            route.add(forwardPort);
                            BmulticastMessage(portStr + "\t" + key + "\t" + value + "\t" + "INSERT", route);
                        }
                        } else {
                        db.insertWithOnConflict(mDbHelper.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
                        contents.put(key,value);
                    }
                } else if(nodeID.compareTo(succ)>0) {   // Loop around the Ring
                    if (nodeID.compareTo(HashIt) > 0 && succ.compareTo(HashIt) < 0) {

                        db.insertWithOnConflict(mDbHelper.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
                        contents.put(key,value);
                    } else {

                        route = new ArrayList<String>();
                        if (globalList.size() > 1) {
                            me = globalList.indexOf(portStr);
                            if (me == 0) {
                                forwardPort = String.valueOf(Integer.parseInt(globalList.get(me + 1)) * 2);

                            } else if (me == globalList.size() - 1) {
                                forwardPort = String.valueOf(Integer.parseInt(globalList.get(0)) * 2);

                            } else {
                                forwardPort = String.valueOf(Integer.parseInt(globalList.get(me + 1)) * 2);

                            }

                            route.add(forwardPort);

                            BmulticastMessage(portStr + "\t" + key + "\t" + value + "\t" + "INSERT", route);
                        }
                        }
                }
            }
            // corner cases to resolve conflicts
            else if (nodeID.compareTo(HashIt) >= 0 && HashIt.compareTo(pred) > 0) {
                //System.out.println("INSERT LOCAL");
                db.insertWithOnConflict(mDbHelper.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
                contents.put(key,value);
            } else if (HashIt.compareTo(nodeID) < 0 && nodeID.compareTo(pred) < 0) {
                db.insertWithOnConflict(mDbHelper.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
                contents.put(key,value);
            } else if (nodeID.compareTo(HashIt) < 0 && pred.compareTo(HashIt) < 0 && nodeID.compareTo(pred) < 0) {

                db.insertWithOnConflict(mDbHelper.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
                contents.put(key,value);
            } else {

                route = new ArrayList<String>();
            if (globalList.size() > 1) {
                me = globalList.indexOf(portStr);
                if (me == 0) {
                    forwardPort = String.valueOf(Integer.parseInt(globalList.get(me + 1)) * 2);
                } else if (me == globalList.size() - 1) {
                    forwardPort = String.valueOf(Integer.parseInt(globalList.get(0)) * 2);
                } else {
                    forwardPort = String.valueOf(Integer.parseInt(globalList.get(me + 1)) * 2);
                }

                route.add(forwardPort);
                BmulticastMessage(portStr + "\t" + key + "\t" + value + "\t" + "INSERT", route);
            }

            }

        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        mDbHelper = new FeedReaderDBHelper(getContext());
        // need to create the server and client threads
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        contents = new HashMap<String,String>();
        System.out.println("RESTART");
        NodeList = new ArrayList<String>();
        myself=0;
        // Create Server Async Task
        try {
            nodeID = genHash(portStr);
        }
        catch(NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create serverSocket");
        }

        mContentResolver= (this.getContext()).getContentResolver();
        temp = new HashMap<String,String>();
        String msg= myPort + "\t" + portStr+"\t"+ "JOIN";
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);

        return true;
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        String[] columnNames={"key","value"};
        m=new MatrixCursor(columnNames);
        SQLiteDatabase db = mDbHelper.getReadableDatabase();
        SQLiteQueryBuilder builder = new SQLiteQueryBuilder();
        builder.setTables(FeedReaderDBHelper.TABLE_NAME);
        Cursor cursor=null;


        if(pred==(null) && succ ==(null)){
        //if(count==-1 || count ==0 || portStr=="5554"){
            if (selection.equals("\"@\"") || selection.equals("\"*\"")) {

                cursor = builder.query(db, null, null, null, null, null, null);
                return cursor;
            }
            else{
                cursor = builder.query(db, null, "key=" + "'" + selection + "'", null, null, null, null);

                return cursor;
            }
        }
        if(count>0){
            if(selection.equals("\"@\"")){
                cursor = builder.query(db, null, null, null, null, null, null);

                return cursor;
            }
            else if(selection.equals("\"*\"")){

                route = new ArrayList<String>();
                for(int i=0;i<=globalList.size()-1;i++){
                    if(globalList.get(i)!=portStr){
                        route.add(String.valueOf(Integer.parseInt(globalList.get(i))*2));
                    }
                  }


                for (Map.Entry<String, String> entry : contents.entrySet()) {
                    String[] str = new String[2];
                    str[0]=entry.getKey();
                    str[1]=entry.getValue();
                    m.addRow(str);
                }

                BmulticastMessage("TORTURE"+"\t"+myPort,route);
                try {
                    Thread.sleep(10200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                for(Map.Entry<String, String> entry : temp.entrySet()){
                    String[] str = new String[2];
                    str[0]=entry.getKey();
                    str[1]=entry.getValue();
                    m.addRow(str);
                }

                return m;
            }
            else{

                if(contents.containsKey(selection)){    // if key in local
                    // return it

                    cursor = builder.query(db, null, "key=" + "'" + selection + "'", null, null, null, null);
                        return cursor;
                    }

                else{
                    // send to next

                    QueryArgs.append("QUERY"+"\t"+ selection);
                    QueryArgs.append("\t"+myPort+",");

                     route = new ArrayList<String>();
                        for(int i=0;i<=globalList.size()-1;i++){
                            route.add(String.valueOf(Integer.parseInt(globalList.get(i))*2));
                        }

                    BmulticastMessage(QueryArgs.toString(), route);
                    try {
                        Thread.sleep(1500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    QueryArgs.delete(0,QueryArgs.length());
                    return m;
                }

            }
        }
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    // Server and Client Tasks
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];


            try {

                while (true) {
                    Socket listener = serverSocket.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(listener.getInputStream()));
                    String line = null;
                    while ((line = in.readLine()) != null) {
                        line = line.trim();
                        if (line.contains("JOIN")) {
                            count++;
                            lst.add(line.split("\t")[1]); // contains AVD NUMBER
                            try {
                                a = genHash(line.split("\t")[1]); // HASH THE AVD NUMBER
                                list.add(a); // STORE IN LIST

                            } catch (NoSuchAlgorithmException e) {
                                e.printStackTrace();
                            }

                            ports.add(line.split("\t")[0]);
                            //String tokens[] = line.split("\t");
                            if (!list.isEmpty()) {

                                for (int i = 0; i <= list.size() - 1; i++) {
                                    Message.append(list.get(i) + ",");

                                }
                            }
                            Message.append("\t");
                            Message.append("SPARTAAA");
                            BmulticastMessage("" + list.size() + "\t" + Message.toString() + "\t", ports);
                            Message.delete(0, Message.length());
                            Collections.sort(list);
                            for (int i = 0; i <= list.size() - 1; i++) {
                                Message.append(list.get(i) + ",");
                            }
                            Message.append("\t");
                            Message.append("GLOBAL_LIST");
                            BmulticastMessage("" + list.size() + "\t" + Message.toString(), ports);
                            Message.delete(0, Message.length());

                        }

                        if (line.contains("GLOBAL_LIST")) {
                            String token[] = line.split("\t");
                            String tokens[] = token[1].split(",");
                            globalList = new ArrayList<String>();
                            for (int i = 0; i <= Integer.parseInt(token[0]) - 1; i++) {

                                globalList.add(lookupSHA1(tokens[i]));
                                //globalList.add(tokens[i]);
                            }
                        }
                        // shit just got real! This is SPARTAAA
                        if (line.contains("SPARTAAA")) {

                            NodeList = new ArrayList<String>();
                            String token[] = line.split("\t");
                            count = Integer.parseInt(token[0]);
                            String HashVal[] = token[1].split(",");
                            for (int i = 0; i <= Integer.parseInt(token[0])-1; i++) {
                                NodeList.add(HashVal[i]);
                            }
                            Collections.sort(NodeList);

                            myself = NodeList.indexOf(nodeID);

                            if (NodeList.size() >1) {
                                if (myself == 0) {
                                    pred = NodeList.get(NodeList.size() - 1);
                                    succ = NodeList.get(myself + 1);
                                } else if (myself == NodeList.size() - 1) {
                                    pred = NodeList.get(myself - 1);
                                    succ = NodeList.get(0);
                                } else {
                                    if(myself==-1){
                                        NodeList.add(lookupSHA1(portStr));
                                        String tmp= lookupSHA1(portStr);
                                        myself= NodeList.indexOf(tmp);
                                        pred = NodeList.get(myself - 1);
                                        succ = NodeList.get(myself + 1);
                                    }
                                    else{
                                        pred = NodeList.get(myself - 1);
                                        succ = NodeList.get(myself + 1);
                                    }

                                }
                            }
                        }

                        if (line.contains("INSERT")) {
                             // line format as KEY VALUE "INSERT"

                            String token[] = line.split("\t");
                            key = token[1];
                            value = token[2];
                            Uri mURI = setupURI("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                            vals.put("key", key);
                            vals.put("value", value);
                            mContentResolver.insert(mURI, vals);

                        }
                        if (line.contains("QUERY")) {
                            // format QUERY KEY PORTS,
                            String token[] = line.split("\t");

                            if (contents.containsKey(token[1])) {
                                // Key found
                                route = new ArrayList<String>();
                                route.add(token[2].split(",")[0]);  // port of sender

                                BmulticastMessage(token[1] + "\t" + contents.get(token[1]) + "\t" + "FINAL", route); // send to sender

                            }
                        }
                        if (line.contains("FINAL")) {

                            String token[] = line.split("\t");
                            String[] str = new String[2];
                            str[0] = token[0];
                            str[1] = token[1];
                            m.addRow(str);

                        }
                        if (line.contains("TORTURE")) {
                            // format TORTURE REQUESTING_PORT
                            StringBuffer br = new StringBuffer();
                            for (Map.Entry<String, String> entry : contents.entrySet()) {
                                br.append(entry.getKey() + "\t" + entry.getValue());
                                try {
                                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(line.split("\t")[1]));
                                    String msgToSend = br + "\t" + "COPYBACK";
                                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                                    msgToSend = msgToSend.trim();
                                    out.print(msgToSend);
                                    out.flush();
                                    socket.close();

                                } catch (UnknownHostException e) {
                                    e.printStackTrace();
                                }
                                br.delete(0, br.length());
                            }
                        }
                        if(line.contains("DELETE ALL")){
                            SQLiteDatabase db = mDbHelper.getWritableDatabase();
                            db.delete(mDbHelper.TABLE_NAME, null, null);
                            contents= new HashMap<String,String>();
                        }
                        if(line.contains("DELETE_SELECTION")){
                                if(contents.containsKey(line.split("\t")[1])){
                                    SQLiteDatabase db = mDbHelper.getWritableDatabase();
                                    db.delete(mDbHelper.TABLE_NAME, "key=" + "'" + line.split("\t")[1] + "'", null);
                                    contents.remove(line.split("\t")[1]);
                                }
                        }

                        if (line.contains("COPYBACK")) {

                            // format Key +"\t"+ Value + .
                            String token[] = line.split("\t");
                            temp.put(token[0], token[1]);
                           }
                        publishProgress(line);
                    }
                    listener.close();
                }
            }

                catch(UnknownHostException e1){
                    e1.printStackTrace();
                }catch(IOException e1){
                    e1.printStackTrace();
                }
                return null;
            }

        }

    private String lookupSHA1(String token) {
        String PortNumber=null;
        if(token.equals("33d6357cfaaf0f72991b0ecd8c56da066613c089")){
            PortNumber= "5554";
        }
        if(token.equals("208f7f72b198dadd244e61801abe1ec3a4857bc9")){
            PortNumber="5556";
        }
        if(token.equals("c25ddd596aa7c81fa12378fa725f706d54325d12")){
            PortNumber="5560";
        }
        if(token.equals("177ccecaec32c54b82d5aaafc18a2dadb753e3b1")){
            PortNumber="5562";
        }
        if(token.equals("abf0fd8db03e5ecb199a9b82929e9db79b909643")){
            PortNumber="5558";
        }

        return PortNumber;
    }

    public void BmulticastMessage(String msg, ArrayList<String> pt){

        for(int i = 0; i <=pt.size()-1; i++) {
            try {
                // send happens here
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(pt.get(i)));
                String msgToSend = msg;
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                msgToSend = msgToSend.trim();
                out.print(msgToSend);
                out.flush();
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
        }
    }

        private Uri setupURI(String content, String s) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(s);
            uriBuilder.scheme(content);
            return uriBuilder.build();
        }


        private class ClientTask extends AsyncTask<String, Void, Void> {
            @Override
            protected Void doInBackground(String... msgs) {
                //build a string array of ports to send to
                String remotePort = "11108";
                try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort));
                        String msgToSend = msgs[0];
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                        msgToSend = msgToSend.trim();
                        out.print(msgToSend);
                        out.flush();
                        socket.close();
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "ClientTask UnknownHostException");
                    } catch (IOException e) {
                        Log.e(TAG, "ClientTask socket IOException");
                    }


                return null;

            }
        }


    }

