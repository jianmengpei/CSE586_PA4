package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	String [] avdHash = {"","","","",""};
	String [] avdNum = {"5562","5556","5554","5558","5560"};
	String selfAvdNum;
	private  ContentValues[] mContentValuesBase;
	private  ContentValues[] preContentValuesBase;
	private  ContentValues[] prepreContentValuesBase;
	private static final int TEST_CNT = 50;
	private static int count = 0, precount = 0, preprecount = 0;
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	final String[] matrix = {"key","value"};
	static final int server_port = 10000;
	private boolean queryall = false, queryone = false;
	private String queryallresult = null, queryoneresult = null;
	int pre=0, prepre=0, suc=0;
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		Log.i("delete",selection);
		if(selection.equals("*")){
			count = 0;
			//SendMsg(String.valueOf(Integer.valueOf(selfavd) * 2), String.valueOf(Integer.valueOf(successor) * 2), "DeleteAll",null, null,null,null,null);
			boolean coordinatoralive = true;

			//send the msg to the next to delete all the contentValues
			Socket socket = null;
			try {
				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[suc]) * 2);
				MsgToSend msgToSend = SetMsgToSend("DeleteAllFirstTrail", null, null, null, null,null,selfAvdNum);
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
				objectOutputStream.writeObject(msgToSend);
				objectOutputStream.flush();
			}catch (UnknownHostException e){
				e.printStackTrace();
			}catch (IOException e){
				e.printStackTrace();
			}
			try {
				socket.setSoTimeout(200);
				ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
				MsgToSend receviedMsg = (MsgToSend)inputStream.readObject();
				if(receviedMsg.getType().equals("DeleteSucceed")){
					Log.i("DeleteFirst","Succeeds");
				}
				coordinatoralive = true;
				socket.close();
			}catch(SocketException e){
				e.printStackTrace();
				try{
					coordinatoralive = false;
					socket.close();
				}catch (IOException e2){
					e2.printStackTrace();
				}
			}catch (IOException e){
				e.printStackTrace();
			}catch (ClassNotFoundException e){
				e.printStackTrace();
			}
			if(!coordinatoralive ) {
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[(suc + 1) % 5]) * 2);
					MsgToSend msgToSend = SetMsgToSend("DeleteAllSecondTrail", null, null, null, null, null, selfAvdNum);
					ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
					outputStream.writeObject(msgToSend);
					outputStream.flush();

					ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
					MsgToSend receviedMsg = (MsgToSend) inputStream.readObject();
					if (receviedMsg.getType().equals("DeleteSucceed")) {
						Log.i("Delete", "Succeed");
					}
					socket.close();
					Log.i("DeleteTrial", receviedMsg.getValue());
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}else if(selection.equals("@")){
			count = 0;
		}else{
			for(int i=0; i<count; i++){
				if(mContentValuesBase[i].getAsString(KEY_FIELD).equals(selection)){
					for(int j=i; j<count-1; j++){
						mContentValuesBase[j]=mContentValuesBase[j+1];
					}
					count--;
					return 0;
				}
			}
			int des = 0;
			try {
				String selectionHash = genHash(selection);
				for (int i = 0; i < 5; i++) {
					if (avdHash[i].compareTo(selectionHash) < 0) continue;
					else {
						des = i;
						break;
					}
				}
				if (avdHash[4].compareTo(selectionHash) < 0) des = 0;
			}catch (NoSuchAlgorithmException e){
				e.printStackTrace();
			}
			boolean coordinatoralive = true;
			Socket socket = null;
			try {
				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[des])*2);
				MsgToSend msgToSend = SetMsgToSend("DeleteOneFirstTrail", selection, null,null, null, null,null);
				ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
				outputStream.writeObject(msgToSend);
				outputStream.flush();
			}catch (UnknownHostException e){
				e.printStackTrace();
			}catch (IOException e){
				e.printStackTrace();
			}
			try {
				socket.setSoTimeout(200);
				ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
				MsgToSend receviedMsg = (MsgToSend)inputStream.readObject();
				if(receviedMsg.getType().equals("DeleteSucceed")){
					queryoneresult = receviedMsg.getKey() +  "\n" + receviedMsg.getValue()+"\n";
					Log.i("Delete","Succeeds");
				}
				Log.i("DeleteOneFirstTrail",receviedMsg.getValue());
				coordinatoralive = true;
				socket.close();
			}catch(SocketException e){
				e.printStackTrace();
				try{
					coordinatoralive = false;
					socket.close();
				}catch (IOException e2){
					e2.printStackTrace();
				}
			}catch (IOException e){
				e.printStackTrace();
			}catch (ClassNotFoundException e){
				e.printStackTrace();
			}

			if(!coordinatoralive ) {
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[(des + 1) % 5]) * 2);
					MsgToSend msgToSend = SetMsgToSend("DeleteOneSecondTrail", selection, null,null, null,null,null);
					ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
					outputStream.writeObject(msgToSend);
					outputStream.flush();

					ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
					MsgToSend receviedMsg = (MsgToSend) inputStream.readObject();
					if(receviedMsg.getType().equals("DeleteOneSucceed")){
						queryoneresult = receviedMsg.getKey() +  "\n" + receviedMsg.getValue()+"\n";
						Log.i("DeleteOne","Succeed");
					}
					socket.close();
					Log.i("DeleteOneSecondTrail", receviedMsg.getValue());
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}

			//SendMsg(String.valueOf(Integer.valueOf(selfavd) * 2), String.valueOf(Integer.valueOf(successor) * 2), "DeleteOne",selection, null,null,null,null);
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
		//1, genHash of the key of the values, and send it to the appropiate nodes if not owns
		int des = 0;
		Socket socket=null;
		boolean coordinatoralive = true;
		try{
			String conKeyHash = genHash(values.getAsString(KEY_FIELD));
			for(int i=0; i<5; i++){
				if(avdHash[i].compareTo(conKeyHash)<0) continue;
				else {
					des = i;
					break;
				}
			}
			if(avdHash[4].compareTo(conKeyHash)<0) des = 0;
			//build the socket and send the values to the nodes
			//If the message is sent successfully, wait for the response
			//else, find the next node to send
		}catch (NoSuchAlgorithmException e){
			e.printStackTrace();
		}
		try {
			socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[des])*2);
			MsgToSend msgToSend = SetMsgToSend("FirstTrail", values.getAsString(KEY_FIELD), values.getAsString(VALUE_FIELD),null, null, null,null);
			ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
			outputStream.writeObject(msgToSend);
			outputStream.flush();
		}catch (UnknownHostException e){
				e.printStackTrace();
		}catch (IOException e){
				e.printStackTrace();
		}
		try {
			socket.setSoTimeout(200);
			ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
			MsgToSend receviedMsg = (MsgToSend)inputStream.readObject();
			if(receviedMsg.getType().equals("InsertSucceed")){
				Log.i("Insert","Succeeds");
			}
			Log.i("SecondTrial",receviedMsg.getValue());
			coordinatoralive = true;
			socket.close();
		}catch(SocketException e){
			e.printStackTrace();
			try{
				coordinatoralive = false;
				socket.close();
			}catch (IOException e2){
				e2.printStackTrace();
			}
		}catch (IOException e){
			e.printStackTrace();
		}catch (ClassNotFoundException e){
			e.printStackTrace();
		}

		if(!coordinatoralive ) {
			try {
				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[(des + 1) % 5]) * 2);
				MsgToSend msgToSend = SetMsgToSend("SecondTrail", values.getAsString(KEY_FIELD), values.getAsString(VALUE_FIELD),null, null,null,null);
				ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
				outputStream.writeObject(msgToSend);
				outputStream.flush();

				ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
				MsgToSend receviedMsg = (MsgToSend) inputStream.readObject();
				if(receviedMsg.getType().equals("InsertSucceed")){
					Log.i("Insert2","Succeed");
				}
				socket.close();
				Log.i("SecondTrial", receviedMsg.getValue());
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}

		return null;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		mContentValuesBase = new ContentValues[TEST_CNT];
		preContentValuesBase = new ContentValues[TEST_CNT];
		prepreContentValuesBase = new ContentValues[TEST_CNT];
		mContentValuesBase = initTestValues();
		preContentValuesBase = initTestValues();
		prepreContentValuesBase = initTestValues();

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		selfAvdNum = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

		for(int i=0; i<5; i++){
			try{
				avdHash[i] = genHash(avdNum[i]);
			}catch(NoSuchAlgorithmException e){
				e.printStackTrace();
			}
		}
		Log.i("selfavdNum",selfAvdNum);
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "AddNode", null, null);

		try {
			ServerSocket serverSocket = new ServerSocket();
			serverSocket.setReuseAddress(true);
			serverSocket.bind(new InetSocketAddress(server_port));
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket, null, null);
		}catch (IOException e){
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		Log.i("slection",selection);
		MatrixCursor cs = new MatrixCursor(matrix, 1);
		queryoneresult = null;
		queryallresult = null;

		if(selection.equals("*")){
			boolean coordinatoralive = true;
			String msg = "";
			for(int i=0; i<count; i++){
				msg = msg + mContentValuesBase[i].getAsString(KEY_FIELD) +"\n" + mContentValuesBase[i].getAsString(VALUE_FIELD)+"\n";
			}

			//send the msg to the next to collect the contentvalues, and serverTask set a signal
			Socket socket = null;
			try {
				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[suc]) * 2);
				MsgToSend msgToSend = SetMsgToSend("QueryAllFirstTrail", null, null, null, null,msg,selfAvdNum);
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
				objectOutputStream.writeObject(msgToSend);
				objectOutputStream.flush();
			}catch (UnknownHostException e){
				e.printStackTrace();
			}catch (IOException e){
				e.printStackTrace();
			}
			try {
				socket.setSoTimeout(200);
				ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
				MsgToSend receviedMsg = (MsgToSend)inputStream.readObject();
				if(receviedMsg.getType().equals("QuerySucceed")){
					Log.i("QueryFirst","Succeeds");
				}
				coordinatoralive = true;
				socket.close();
			}catch(SocketException e){
				e.printStackTrace();
				try{
					coordinatoralive = false;
					socket.close();
				}catch (IOException e2){
					e2.printStackTrace();
				}
			}catch (IOException e){
				e.printStackTrace();
			}catch (ClassNotFoundException e){
				e.printStackTrace();
			}
			if(!coordinatoralive ) {
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[(suc + 1) % 5]) * 2);
					MsgToSend msgToSend = SetMsgToSend("QueryAllSecondTrail", null, null, null, null, msg, selfAvdNum);
					ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
					outputStream.writeObject(msgToSend);
					outputStream.flush();

					ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
					MsgToSend receviedMsg = (MsgToSend) inputStream.readObject();
					if (receviedMsg.getType().equals("QuerySucceed")) {
						Log.i("Query2", "Succeed");
					}
					socket.close();
					Log.i("SecondTrial", receviedMsg.getValue());
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}

			while(!queryall){

			}
			queryall = false;
			String []seperated = queryallresult.split("\n");

			for(int i=0; i<seperated.length;i+=2){
				cs.addRow(new Object[]{seperated[i],seperated[i+1]});
				Log.i("queryall", seperated[i]+" "+seperated[i+1]);
			}
		}else if(selection.equals("@")){
			for(int i=0; i<count; i++){
				cs.addRow(new Object[]{mContentValuesBase[i].getAsString(KEY_FIELD), mContentValuesBase[i].getAsString(VALUE_FIELD)});
			}
		}else{
			//Log.i("queryone","it starts");
			for(int i=0; i<count; i++){
				if(mContentValuesBase[i].getAsString(KEY_FIELD).equals(selection)){
					cs.addRow(new Object[]{mContentValuesBase[i].getAsString(KEY_FIELD),mContentValuesBase[i].getAsString(VALUE_FIELD)});
					Log.i("queryone1", mContentValuesBase[i].getAsString(KEY_FIELD) + " " + mContentValuesBase[i].getAsString(VALUE_FIELD));
					return cs;
				}
			}
			int des = 0;
			try {
				String selectionHash = genHash(selection);
				for (int i = 0; i < 5; i++) {
					if (avdHash[i].compareTo(selectionHash) < 0) continue;
					else {
						des = i;
						break;
					}
				}
				if (avdHash[4].compareTo(selectionHash) < 0) des = 0;
			}catch (NoSuchAlgorithmException e){
				e.printStackTrace();
			}
			boolean coordinatoralive = true;
			Socket socket = null;
			try {
				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[des])*2);
				MsgToSend msgToSend = SetMsgToSend("QueryOneFirstTrail", selection, null,null, null, null,null);
				ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
				outputStream.writeObject(msgToSend);
				outputStream.flush();
			}catch (UnknownHostException e){
				e.printStackTrace();
			}catch (IOException e){
				e.printStackTrace();
			}
			try {
				socket.setSoTimeout(200);
				ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
				MsgToSend receviedMsg = (MsgToSend)inputStream.readObject();
				if(receviedMsg.getType().equals("QuerySucceed")){
					queryoneresult = receviedMsg.getKey() +  "\n" + receviedMsg.getValue()+"\n";
					Log.i("Query","Succeeds");
				}
				Log.i("QueryAllFirstTrail",receviedMsg.getValue());
				coordinatoralive = true;
				socket.close();
			}catch(SocketException e){
				e.printStackTrace();
				try{
					coordinatoralive = false;
					socket.close();
				}catch (IOException e2){
					e2.printStackTrace();
				}
			}catch (IOException e){
				e.printStackTrace();
			}catch (ClassNotFoundException e){
				e.printStackTrace();
			}

			if(!coordinatoralive ) {
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[(des + 1) % 5]) * 2);
					MsgToSend msgToSend = SetMsgToSend("QueryAllSecondTrail", selection, null,null, null,null,null);
					ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
					outputStream.writeObject(msgToSend);
					outputStream.flush();

					ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
					MsgToSend receviedMsg = (MsgToSend) inputStream.readObject();
					if(receviedMsg.getType().equals("QuerySucceed")){
						queryoneresult = receviedMsg.getKey() +  "\n" + receviedMsg.getValue()+"\n";
						Log.i("QeuryOne","Succeed");
					}
					socket.close();
					Log.i("QueryAllSecondTrail", receviedMsg.getValue());
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}

			String []seperated = queryoneresult.split("\n");
			cs.addRow(new Object[]{seperated[0], seperated[1]});
			Log.i("queryone2", seperated[0] + " " + seperated[1]);
		}

		//cs.addRow(new Object[]{keyresult, valueresult});
		if(cs == null)
		{
			Log.v("matrixcursor is ", "null");
		}

		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
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
	private ContentValues[] initTestValues() {
		ContentValues[] cv = new ContentValues[TEST_CNT];
		for (int i = 0; i < TEST_CNT; i++) {
			cv[i] = new ContentValues();
		}

		return cv;
	}
	private class ClientTask extends AsyncTask<String, Void, Void>{
		@Override
		protected Void doInBackground(String ...msgs){
			//ask pre and prepre, ask suc
			//ask pre
			for(int i=0; i<5; i++){
				if(avdNum[i].equals(selfAvdNum)){
					pre = (i+5-1)%5;
					prepre = (pre+5-1)%5;
					suc = (i+1)%5;
					break;
				}
			}
			Socket socket = null;
			try {
				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[pre]) * 2);
				Log.i("port number pre","is "+ Integer.parseInt(avdNum[pre]) * 2);
				MsgToSend msgToSend = SetMsgToSend("GiveMePre", null, null,null, null,null,selfAvdNum);
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
				objectOutputStream.writeObject(msgToSend);
				objectOutputStream.flush();

				ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
				MsgToSend receivedMsg = (MsgToSend)objectInputStream.readObject();
				preContentValuesBase = SetPreContent(receivedMsg.getKeys(), receivedMsg.getValues());
				socket.close();
			}catch (UnknownHostException e){
				e.printStackTrace();
			}catch (IOException e){
				e.printStackTrace();
			}catch (ClassNotFoundException e){
				e.printStackTrace();
			}

			try {
				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[prepre]) * 2);
				Log.i("port number prepre","is "+ Integer.parseInt(avdNum[prepre]) * 2);
				MsgToSend msgToSend = SetMsgToSend("GiveMePrePre", null, null,null, null,null,selfAvdNum);
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
				objectOutputStream.writeObject(msgToSend);
				objectOutputStream.flush();

				ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
				MsgToSend receivedMsg = (MsgToSend)objectInputStream.readObject();
				prepreContentValuesBase = SetPreContent(receivedMsg.getKeys(), receivedMsg.getValues());
				socket.close();
			}catch (UnknownHostException e){
				e.printStackTrace();
			}catch (IOException e){
				e.printStackTrace();
			}catch (ClassNotFoundException e){
				e.printStackTrace();
			}

			try {
				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[suc]) * 2);
				Log.i("port number suc","is "+Integer.parseInt(avdNum[suc]) * 2);
				MsgToSend msgToSend = SetMsgToSend("GiveMeMine", null, null,null, null,null,selfAvdNum);
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
				objectOutputStream.writeObject(msgToSend);
				objectOutputStream.flush();

				ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
				MsgToSend receivedMsg = (MsgToSend)objectInputStream.readObject();
				mContentValuesBase = SetPreContent(receivedMsg.getKeys(), receivedMsg.getValues());
				socket.close();
			}catch (UnknownHostException e){
				e.printStackTrace();
			}catch (IOException e){
				e.printStackTrace();
			}catch (ClassNotFoundException e){
				e.printStackTrace();
			}
			return null;
		}

	}
	private class ServerTask extends AsyncTask<ServerSocket, Void, Void>{

		@Override
		protected Void doInBackground(ServerSocket ... sockets){
			ServerSocket serverSocket = sockets[0];
			try{
				while(true){
					Socket socket= serverSocket.accept();
					//Log.i("serverSocket",socket.toString());
					new Thread(new MultiServer(socket)).start();
				}
			}catch (IOException e){
				e.printStackTrace();
			}
			return null;
		}
	}
	public class MultiServer implements  Runnable{
		private Socket socket = null;
		public MultiServer (Socket socket){
			this.socket = socket;
		}
		public void run(){
			int []next = {0,0};
			try {
				ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
				MsgToSend receivedMsg = (MsgToSend) objectInputStream.readObject();
				Log.i("type and avd", receivedMsg.getOriginal()+receivedMsg.getType());
				if (receivedMsg.getType().equals("FirstTrail")) {
					//store the key into own contentValues, and pass it to the next two nodes
					ContentValues contentValues = new ContentValues();
					contentValues.put(KEY_FIELD, receivedMsg.getKey());
					contentValues.put(VALUE_FIELD, receivedMsg.getValue());
					mContentValuesBase[count++] = contentValues;

					for (int i = 0; i < 5; i++) {
						if (avdNum[i].equals(selfAvdNum)) {
							next[0] = (i + 1) % 5;
							next[1] = (next[0] + 1) % 5;
						}
					}
					//send to the next and next of next node
					for (int i = 0; i < 1; i++) {
						Socket newSocket = null;
						MsgToSend msgToSend = null;
						if (i == 0) {
							msgToSend = SetMsgToSend("PreAvdInsert", contentValues.getAsString(KEY_FIELD),
									contentValues.getAsString(VALUE_FIELD), null, null, null, null);
						} else
							msgToSend = SetMsgToSend("PrePreAvdInsert", contentValues.getAsString(KEY_FIELD),
									contentValues.getAsString(VALUE_FIELD), null, null, null, null);
						try {
							newSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[next[i]]) * 2);
							ObjectOutputStream newoutputStream = new ObjectOutputStream(newSocket.getOutputStream());
							newoutputStream.writeObject(msgToSend);
							newoutputStream.flush();
						} catch (UnknownHostException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}
						try {
							newSocket.setSoTimeout(200);
							ObjectInputStream newinputStream = new ObjectInputStream(newSocket.getInputStream());
							MsgToSend receviedMsg = (MsgToSend) newinputStream.readObject();
							if (receviedMsg.getType().equals("InsertSucceed")) {
								Log.i("Insert3", "Succeeds");
							}
							Log.i("SecondTrial", receviedMsg.getValue());
							newSocket.close();
						} catch (SocketException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						}
					}
					//after receiving the feedback from the next two nodes, the node could return the value.
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
					MsgToSend msgToSend = SetMsgToSend("InsertSucceed", contentValues.getAsString(KEY_FIELD),
							contentValues.getAsString(VALUE_FIELD), null, null, null, null);
					objectOutputStream.writeObject(msgToSend);
					objectOutputStream.flush();

				} else if (receivedMsg.getType().equals("SecondTrial")) {
					ContentValues contentValues = new ContentValues();
					contentValues.put(KEY_FIELD, receivedMsg.getKey());
					contentValues.put(VALUE_FIELD, receivedMsg.getValue());
					preContentValuesBase[precount++] = contentValues;
					for (int i = 0; i < 5; i++) {
						if (avdNum[i].equals(selfAvdNum)) {
							next[0] = (i + 1) % 5;
						}
					}
					Socket newSocket = null;
					try {
						newSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[next[0]]) * 2);
						MsgToSend newmsgToSend = SetMsgToSend("PrePreAvdInsert", contentValues.getAsString(KEY_FIELD),
								contentValues.getAsString(VALUE_FIELD), null, null, null, null);
						ObjectOutputStream newoutputStream = new ObjectOutputStream(newSocket.getOutputStream());
						newoutputStream.writeObject(newmsgToSend);
						newoutputStream.flush();
						ObjectInputStream newinputStream = new ObjectInputStream(newSocket.getInputStream());
						MsgToSend newreceivedMsg = (MsgToSend) newinputStream.readObject();
						if (newreceivedMsg.getType().equals("InsertSucceed")) {
							Log.i("Insert3", "Succeeds");
						}
						Log.i("SecondTrial", newreceivedMsg.getValue());
						newSocket.close();
					} catch (SocketException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
					MsgToSend msgToSend = SetMsgToSend("InsertSucceed", contentValues.getAsString(KEY_FIELD),
							contentValues.getAsString(VALUE_FIELD), null, null, null, null);
					objectOutputStream.writeObject(msgToSend);
					objectOutputStream.flush();

				} else if (receivedMsg.getType().equals("PreAvdInsert")) {
					ContentValues contentValues = new ContentValues();
					contentValues.put(KEY_FIELD, receivedMsg.getKey());
					contentValues.put(VALUE_FIELD, receivedMsg.getValue());
					preContentValuesBase[precount++] = contentValues;
					MsgToSend msgToSend = SetMsgToSend("InsertSucceed", contentValues.getAsString(KEY_FIELD),
							contentValues.getAsString(VALUE_FIELD), null, null, null, null);
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
					objectOutputStream.writeObject(msgToSend);
					objectOutputStream.flush();

				} else if (receivedMsg.getType().equals("PrePreAvdInsert")) {
					ContentValues contentValues = new ContentValues();
					contentValues.put(KEY_FIELD, receivedMsg.getKey());
					contentValues.put(VALUE_FIELD, receivedMsg.getValue());
					prepreContentValuesBase[preprecount++] = contentValues;
					MsgToSend msgToSend = SetMsgToSend("InsertSucceed", contentValues.getAsString(KEY_FIELD),
							contentValues.getAsString(VALUE_FIELD), null, null, null, null);
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
					objectOutputStream.writeObject(msgToSend);
					objectOutputStream.flush();

				} else if (receivedMsg.getType().equals("GiveMePre") || receivedMsg.getType().equals("GiveMePrePre")) {
					String[] keys = new String[count];
					String[] values = new String[count];
					for (int i = 0; i < count; i++) {
						keys[i] = mContentValuesBase[i].getAsString(KEY_FIELD);
						values[i] = mContentValuesBase[i].getAsString(VALUE_FIELD);
					}
					MsgToSend msgToSend = SetMsgToSend("GiveYouMine", null, null, keys, values, null, null);
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
					objectOutputStream.writeObject(msgToSend);
					objectOutputStream.flush();
					Log.i("GiveMePre&PrePre", "send out");


				} else if (receivedMsg.getType().equals("GiveMeMine")) {
					String[] keys = new String[precount];
					String[] values = new String[precount];
					for (int i = 0; i < precount; i++) {
						keys[i] = preContentValuesBase[i].getAsString(KEY_FIELD);
						values[i] = preContentValuesBase[i].getAsString(VALUE_FIELD);
					}
					MsgToSend msgToSend = SetMsgToSend("GiveYouMinePre", null, null, keys, values, null, null);
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
					objectOutputStream.writeObject(msgToSend);
					objectOutputStream.flush();
					Log.i("GiveMeMine", "send out");

				} else if (receivedMsg.getType().equals("QueryAllFirstTrail") || receivedMsg.getType().equals("QueryAllSecondTrail")) {
					//and return to the socket.output query succeeds
					//check whether this is the destination
					//put mContentValues into the msg
					//and pass it to the next nodes
					//if next nodes fails, the pass it to the nextnext nodes
					MsgToSend msgToSend = SetMsgToSend("QuerySucceed", null, null, null, null, null, null);
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
					objectOutputStream.writeObject(msgToSend);
					objectOutputStream.flush();

					if (receivedMsg.getOriginal().equals(selfAvdNum)) {
						queryall = true;
						queryallresult = receivedMsg.getMsg();
					} else {
						String msg = receivedMsg.getMsg();
						if (receivedMsg.getType().equals("QueryAllFirstTrail")) {
							for (int i = 0; i < count; i++) {
								msg = msg + mContentValuesBase[i].getAsString(KEY_FIELD) + "\n" + mContentValuesBase[i].getAsString(VALUE_FIELD) + "\n";
							}
						} else if (receivedMsg.getType().equals("QueryAllSecondTrail")) {
							for (int i = 0; i < precount; i++) {
								msg = msg + preContentValuesBase[i].getAsString(KEY_FIELD) + "\n" + preContentValuesBase[i].getAsString(VALUE_FIELD) + "\n";
							}
							for (int i = 0; i < count; i++) {
								msg = msg + mContentValuesBase[i].getAsString(KEY_FIELD) + "\n" + mContentValuesBase[i].getAsString(VALUE_FIELD) + "\n";
							}
						}
						boolean coordinatoralive = true;
						Socket newsocket = null;
						try {
							newsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[suc]) * 2);
							MsgToSend newmsgToSend = SetMsgToSend("QueryAllFirstTrail", null, null, null, null, msg, receivedMsg.getOriginal());
							ObjectOutputStream newobjectOutputStream = new ObjectOutputStream(newsocket.getOutputStream());
							newobjectOutputStream.writeObject(newmsgToSend);
							newobjectOutputStream.flush();
						} catch (UnknownHostException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}
						try {
							newsocket.setSoTimeout(200);
							ObjectInputStream newinputStream = new ObjectInputStream(newsocket.getInputStream());
							MsgToSend receviedMsg = (MsgToSend) newinputStream.readObject();
							if (receviedMsg.getType().equals("QuerySucceed")) {
								Log.i("QueryFirst", "Succeeds");
							}
							coordinatoralive = true;
							newsocket.close();
						} catch (SocketException e) {
							e.printStackTrace();
							try {
								coordinatoralive = false;
								newsocket.close();
							} catch (IOException e2) {
								e2.printStackTrace();
							}
						} catch (IOException e) {
							e.printStackTrace();
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						}
						if (!coordinatoralive) {
							try {
								newsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[(suc + 1) % 5]) * 2);
								MsgToSend newmsgToSend = SetMsgToSend("QueryAllSecondTrail", null, null, null, null, msg, receivedMsg.getOriginal());
								ObjectOutputStream newoutputStream = new ObjectOutputStream(newsocket.getOutputStream());
								newoutputStream.writeObject(newmsgToSend);
								newoutputStream.flush();

								ObjectInputStream newinputStream = new ObjectInputStream(newsocket.getInputStream());
								MsgToSend receviedMsg = (MsgToSend) newinputStream.readObject();
								if (receviedMsg.getType().equals("QuerySucceed")) {
									Log.i("QueryAll2", "Succeed");
								}
								newsocket.close();
								Log.i("QueryAllSecondTrial", receviedMsg.getValue());
							} catch (UnknownHostException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							} catch (ClassNotFoundException e) {
								e.printStackTrace();
							}
						}
					}
				} else if (receivedMsg.getType().equals("DeleteAllFirstTrail") || receivedMsg.getType().equals("DeleteAllSecondTrail")) {
					MsgToSend msgToSend = SetMsgToSend("QuerySucceed", null, null, null, null, null, null);
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
					objectOutputStream.writeObject(msgToSend);
					objectOutputStream.flush();
					if (receivedMsg.getType().equals("DeleteAllFirstTrail")) {
						count = 0;
					} else if (receivedMsg.getType().equals("DeleteAllSecondTrail")) {
						precount = 0;
						count = 0;
					}
					if (!receivedMsg.getOriginal().equals(selfAvdNum)) {
						//SendMsg(String.valueOf(Integer.valueOf(selfavd) * 2), String.valueOf(Integer.valueOf(successor) * 2), "DeleteAll",null, null,null,null,null);
						boolean coordinatoralive = true;

						//send the msg to the next to delete all the contentValues
						Socket newsocket = null;
						try {
							newsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[suc]) * 2);
							MsgToSend newmsgToSend = SetMsgToSend("DeleteAllFirstTrail", null, null, null, null, null, receivedMsg.getOriginal());
							ObjectOutputStream newobjectOutputStream = new ObjectOutputStream(newsocket.getOutputStream());
							newobjectOutputStream.writeObject(newmsgToSend);
							newobjectOutputStream.flush();
						} catch (UnknownHostException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}
						try {
							newsocket.setSoTimeout(200);
							ObjectInputStream newinputStream = new ObjectInputStream(newsocket.getInputStream());
							MsgToSend receviedMsg = (MsgToSend) newinputStream.readObject();
							if (receviedMsg.getType().equals("DeleteSucceed")) {
								Log.i("DeleteFirst", "Succeeds");
							}
							coordinatoralive = true;
							newsocket.close();
						} catch (SocketException e) {
							e.printStackTrace();
							try {
								coordinatoralive = false;
								newsocket.close();
							} catch (IOException e2) {
								e2.printStackTrace();
							}
						} catch (IOException e) {
							e.printStackTrace();
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						}
						if (!coordinatoralive) {
							try {
								newsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[(suc + 1) % 5]) * 2);
								MsgToSend newmsgToSend = SetMsgToSend("DeleteAllSecondTrail", null, null, null, null, null, receivedMsg.getOriginal());
								ObjectOutputStream newoutputStream = new ObjectOutputStream(newsocket.getOutputStream());
								newoutputStream.writeObject(newmsgToSend);
								newoutputStream.flush();

								ObjectInputStream newinputStream = new ObjectInputStream(newsocket.getInputStream());
								MsgToSend receviedMsg = (MsgToSend) newinputStream.readObject();
								if (receviedMsg.getType().equals("DeleteSucceed")) {
									Log.i("Delete", "Succeed");
								}
								newsocket.close();
								Log.i("DeleteTrial", receviedMsg.getValue());
							} catch (UnknownHostException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							} catch (ClassNotFoundException e) {
								e.printStackTrace();
							}
						}
					}
				} else if (receivedMsg.getType().equals("DeleteOneFirstTrail") || receivedMsg.getType().equals("DeleteOneSecondTrail")) {
					if (receivedMsg.getType().equals("DeleteOneFirstTrail")) {
						for (int i = 0; i < count; i++) {
							if (mContentValuesBase[i].getAsString(KEY_FIELD).equals(receivedMsg.getKey())) {
								for (int j = i; j < count - 1; j++) {
									mContentValuesBase[j] = mContentValuesBase[j + 1];
								}
								count--;
							}
						}
					} else if (receivedMsg.getType().equals("DeleteOneSecondTrail")) {
						for (int i = 0; i < precount; i++) {
							if (preContentValuesBase[i].getAsString(KEY_FIELD).equals(receivedMsg.getKey())) {
								for (int j = i; j < count - 1; j++) {
									preContentValuesBase[j] = preContentValuesBase[j + 1];
								}
								precount--;
							}
						}
					}
				}
			}catch (IOException e){
				e.printStackTrace();
			}catch (ClassNotFoundException e){
				e.printStackTrace();
			}
		}
	}
	public MsgToSend SetMsgToSend(String type, String key, String value, String [] keys, String [] values, String msg, String original){
		MsgToSend msgToSend = new MsgToSend();
		msgToSend.settype(type);
		msgToSend.setkey(key);
		msgToSend.setvalue(value);
		msgToSend.setKeys(keys);
		msgToSend.setValues(values);
		msgToSend.setMsg(msg);
		msgToSend.setOriginal(original);
		return msgToSend;
	}
	public  ContentValues[] SetPreContent(String []keys, String []values){
		ContentValues [] tem = new ContentValues[TEST_CNT];
		for(int i=0; i<TEST_CNT; i++){
			tem[i] = new ContentValues();
		}
		for(int i=0; i<keys.length;i++){
			tem[i].put(KEY_FIELD, keys[i]);
			tem[i].put(VALUE_FIELD,values[i]);
		}
		return  tem;
	}

}
