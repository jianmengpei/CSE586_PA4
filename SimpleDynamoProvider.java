package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
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
				MsgToSend msgToSend = SetMsgToSend("DeleteAllFirstTrail",null,selfAvdNum);
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
				objectOutputStream.writeObject(msgToSend);
				objectOutputStream.flush();
			}catch (UnknownHostException e){
				e.printStackTrace();
			}catch (IOException e){
				e.printStackTrace();
			}
			try {
				socket.setSoTimeout(2000);
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
					MsgToSend msgToSend = SetMsgToSend("DeleteAllSecondTrail", null, selfAvdNum);
					ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
					outputStream.writeObject(msgToSend);
					outputStream.flush();

					ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
					MsgToSend receviedMsg = (MsgToSend) inputStream.readObject();
					if (receviedMsg.getType().equals("DeleteSucceed")) {
						Log.i("Delete", "Succeed");
					}
					socket.close();
					Log.i("DeleteTrial", receviedMsg.getMsg());
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
			//precount = 0;
			//preprecount = 0;
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
				MsgToSend msgToSend = SetMsgToSend("DeleteOneFirstTrail", selection,null);
				ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
				outputStream.writeObject(msgToSend);
				outputStream.flush();
			}catch (UnknownHostException e){
				e.printStackTrace();
			}catch (IOException e){
				e.printStackTrace();
			}
			try {
				socket.setSoTimeout(2000);
				ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
				MsgToSend receviedMsg = (MsgToSend)inputStream.readObject();
				if(receviedMsg.getType().equals("DeleteSucceed")){
					Log.i("Delete","Succeeds");
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
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[(des + 1) % 5]) * 2);
					MsgToSend msgToSend = SetMsgToSend("DeleteOneSecondTrail", selection, null);
					ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
					outputStream.writeObject(msgToSend);
					outputStream.flush();

					ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
					MsgToSend receviedMsg = (MsgToSend) inputStream.readObject();
					if(receviedMsg.getType().equals("DeleteOneSucceed")){
						Log.i("DeleteOne","Succeed");
					}
					socket.close();
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
				if(avdHash[i].compareTo(conKeyHash)>=0){
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
		if(avdNum[des].equals(selfAvdNum)){
			mContentValuesBase[count++] = values;
			//next pre
			Log.i("RighthereInsert", values.getAsString(KEY_FIELD)+" "+values.getAsString(VALUE_FIELD)+" "+count+" inserted");
			int [] next = {0,0};
			for (int i = 0; i < 5; i++) {
				if (avdNum[i].equals(selfAvdNum)) {
					next[0] = (i + 1) % 5;
					next[1] = (next[0] + 1) % 5;
					break;
				}
			}
			//send to the next and next of next node
			for (int i = 0; i < 1; i++) {
				Socket newSocket = null;
				MsgToSend msgToSend = null;
				if (i == 0) {
					msgToSend = SetMsgToSend("PreAvdInsert", values.getAsString(KEY_FIELD)+"\n"+values.getAsString(VALUE_FIELD)+"\n", null);
				} else
					msgToSend = SetMsgToSend("PrePreAvdInsert", values.getAsString(KEY_FIELD)+"\n"+values.getAsString(VALUE_FIELD)+"\n", null);
				try {
					newSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[next[i]]) * 2);
					ObjectOutputStream newoutputStream = new ObjectOutputStream(newSocket.getOutputStream());
					Log.i("sendto", values.getAsString(KEY_FIELD) + " and " + values.getAsString(VALUE_FIELD) + " send to " + avdNum[next[i]]);
					newoutputStream.writeObject(msgToSend);
					newoutputStream.flush();
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				try {
					newSocket.setSoTimeout(2000);
					ObjectInputStream newinputStream = new ObjectInputStream(newSocket.getInputStream());
					MsgToSend receviedMsg = (MsgToSend) newinputStream.readObject();
					if (receviedMsg.getType().equals("InsertSucceed")) {
						Log.i("Insert3", "Succeeds");
					}
					Log.i("InsertFromHereSucceeds", receviedMsg.getMsg());
					newSocket.close();
				} catch (SocketException e) {
					e.printStackTrace();
					try{
						Log.i("InsertFailed","because "+avdNum[next[i]]+" shot down");
						newSocket.close();
					}catch (IOException e2){
						e2.printStackTrace();
					}
				} catch (IOException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
			//after receiving the feedback from the next two nodes, the node could return the value.
			//nextnext preand pre
		}else {
			try {
				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[des]) * 2);
				Log.i("sendto", values.getAsString(KEY_FIELD) + " and " + values.getAsString(VALUE_FIELD) + " send to " + avdNum[des]);
				MsgToSend msgToSend = SetMsgToSend("InsertFirstTrail", values.getAsString(KEY_FIELD) + "\n" + values.getAsString(VALUE_FIELD) + "\n", null);
				ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
				outputStream.writeObject(msgToSend);
				outputStream.flush();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				socket.setSoTimeout(2000);
				ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
				MsgToSend receviedMsg = (MsgToSend) inputStream.readObject();
				if (receviedMsg.getType().equals("InsertSucceed")) {
					Log.i("Insert", "Succeeds");
				}
				coordinatoralive = true;
				socket.close();
			} catch (SocketException e) {
				e.printStackTrace();
				try {
					coordinatoralive = false;
					socket.close();
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
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[(des + 1) % 5]) * 2);
					MsgToSend msgToSend = SetMsgToSend("InsertSecondTrail", values.getAsString(KEY_FIELD) + "\n" + values.getAsString(VALUE_FIELD) + "\n", null);
					Log.i("sendto", values.getAsString(KEY_FIELD) + " and " + values.getAsString(VALUE_FIELD) + " send to " + avdNum[(des + 1) % 5]);
					ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
					outputStream.writeObject(msgToSend);
					outputStream.flush();

					ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
					MsgToSend receviedMsg = (MsgToSend) inputStream.readObject();
					if (receviedMsg.getType().equals("InsertSucceed")) {
						Log.i("Insert2", "Succeed");
					}
					socket.close();
					//Log.i("InsertSecondTrial", receviedMsg.getMsg());
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
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
			ServerSocket serverSocket = new ServerSocket(server_port);
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
				MsgToSend msgToSend = SetMsgToSend("QueryAllFirstTrail", msg,selfAvdNum);
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
				objectOutputStream.writeObject(msgToSend);
				objectOutputStream.flush();
			}catch (UnknownHostException e){
				e.printStackTrace();
			}catch (IOException e){
				e.printStackTrace();
			}
			try {
				socket.setSoTimeout(2000);
				ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
				MsgToSend receviedMsg = (MsgToSend)inputStream.readObject();
				if(receviedMsg.getType().equals("Iamalive")){
					Log.i("QueryFirst","Iamalive");
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
					MsgToSend msgToSend = SetMsgToSend("QueryAllSecondTrail",  msg, selfAvdNum);
					ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
					outputStream.writeObject(msgToSend);
					outputStream.flush();

					ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
					MsgToSend receviedMsg = (MsgToSend) inputStream.readObject();
					if (receviedMsg.getType().equals("Iamalive")) {
						Log.i("Query2", "Iamalive");
					}
					socket.close();
					//Log.i("SecondTrial", receviedMsg.getMsg());
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
			//including all the pre and prepre
			Log.i("QueryLocal", "Starts");
			for(int i=0; i<count; i++){
				cs.addRow(new Object[]{mContentValuesBase[i].getAsString(KEY_FIELD), mContentValuesBase[i].getAsString(VALUE_FIELD)});
			}
			for(int i=0; i<precount;i++){
				cs.addRow(new Object[]{preContentValuesBase[i].getAsString(KEY_FIELD), preContentValuesBase[i].getAsString(VALUE_FIELD)});
			}
			for(int i=0; i<preprecount;i++){
				cs.addRow(new Object[]{prepreContentValuesBase[i].getAsString(KEY_FIELD), prepreContentValuesBase[i].getAsString(VALUE_FIELD)});
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
				MsgToSend msgToSend = SetMsgToSend("QueryOneFirstTrail", selection,null);
				ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
				outputStream.writeObject(msgToSend);
				outputStream.flush();
			}catch (UnknownHostException e){
				e.printStackTrace();
			}catch (IOException e){
				e.printStackTrace();
			}
			try {
				socket.setSoTimeout(2000);
				ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
				MsgToSend receviedMsg = (MsgToSend)inputStream.readObject();
				if(receviedMsg.getType().equals("Ifound")){
					queryoneresult = receviedMsg.getMsg();
					Log.i("Query","Succeeds");
				}
				Log.i("QueryOneFirstTrail",receviedMsg.getMsg());
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
					MsgToSend msgToSend = SetMsgToSend("QueryOneSecondTrail", selection,null);
					ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
					outputStream.writeObject(msgToSend);
					outputStream.flush();

					ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
					MsgToSend receviedMsg = (MsgToSend) inputStream.readObject();
					if(receviedMsg.getType().equals("Ifound")){
						queryoneresult = receviedMsg.getMsg();
						Log.i("QeuryOneSecondTrail","Succeed");
					}
					socket.close();
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

		return cs;
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
				//Log.i("port number pre","is "+ Integer.parseInt(avdNum[pre]) * 2);
				MsgToSend msgToSend = SetMsgToSend("GiveMeYourmContent",null,selfAvdNum);
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
				objectOutputStream.writeObject(msgToSend);
				objectOutputStream.flush();

				ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
				MsgToSend receivedMsg = (MsgToSend)objectInputStream.readObject();
				preContentValuesBase = SetContent(receivedMsg.getMsg());
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
				//Log.i("port number prepre","is "+ Integer.parseInt(avdNum[prepre]) * 2);
				MsgToSend msgToSend = SetMsgToSend("GiveMeYourmContent",null,selfAvdNum);
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
				objectOutputStream.writeObject(msgToSend);
				objectOutputStream.flush();

				ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
				MsgToSend receivedMsg = (MsgToSend)objectInputStream.readObject();
				prepreContentValuesBase = SetContent(receivedMsg.getMsg());
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
				//Log.i("port number suc","is "+Integer.parseInt(avdNum[suc]) * 2);
				MsgToSend msgToSend = SetMsgToSend("GiveMeYourPre", null,selfAvdNum);
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
				objectOutputStream.writeObject(msgToSend);
				objectOutputStream.flush();

				ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
				MsgToSend receivedMsg = (MsgToSend)objectInputStream.readObject();
				mContentValuesBase = SetContent(receivedMsg.getMsg());
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
				//Log.i("type and avd", receivedMsg.getMsg()+receivedMsg.getType());
				if (receivedMsg.getType().equals("InsertFirstTrail")) {
					//store the key into own contentValues, and pass it to the next two nodes
					ContentValues contentValues = new ContentValues();
					String []seperated = receivedMsg.getMsg().split("\n");
					contentValues.put(KEY_FIELD, seperated[0]);
					contentValues.put(VALUE_FIELD, seperated[1]);
					mContentValuesBase[count++] = contentValues;
					Log.i("countInsert", receivedMsg.getMsg()+" "+count+" inserted");
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
							msgToSend = SetMsgToSend("PreAvdInsert", receivedMsg.getMsg(), null);
						} else
							msgToSend = SetMsgToSend("PrePreAvdInsert", receivedMsg.getMsg(), null);
						try {
							newSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[next[i]]) * 2);
							ObjectOutputStream newoutputStream = new ObjectOutputStream(newSocket.getOutputStream());
							newoutputStream.writeObject(msgToSend);
							newoutputStream.flush();
							Log.i("SendTheMsgTo", msgToSend.getMsg() + " to " + avdNum[next[i]]);
						} catch (UnknownHostException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}
						try {
							newSocket.setSoTimeout(2000);
							ObjectInputStream newinputStream = new ObjectInputStream(newSocket.getInputStream());
							MsgToSend receviedMsg = (MsgToSend) newinputStream.readObject();
							if (receviedMsg.getType().equals("InsertSucceed")) {
								Log.i("Insert3", "Succeeds");
							}
							Log.i("SendTheMsgToSucceed", msgToSend.getMsg() + " to " + avdNum[next[i]]);
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
					MsgToSend msgReturn = SetMsgToSend("InsertSucceed", receivedMsg.getMsg(), null);
					objectOutputStream.writeObject(msgReturn);
					objectOutputStream.flush();

				} else if (receivedMsg.getType().equals("InsertSecondTrial")) {
					ContentValues contentValues = new ContentValues();
					String []seperated = receivedMsg.getMsg().split("\n");
					contentValues.put(KEY_FIELD, seperated[0]);
					contentValues.put(VALUE_FIELD, seperated[1]);
					preContentValuesBase[precount++] = contentValues;
					Log.i("countInsertofpreContent", receivedMsg.getMsg()+" "+count+" inserted");
					for (int i = 0; i < 5; i++) {
						if (avdNum[i].equals(selfAvdNum)) {
							next[0] = (i + 1) % 5;
						}
					}
					Socket newSocket = null;
					try {
						newSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[next[0]]) * 2);
						MsgToSend newmsgToSend = SetMsgToSend("PrePreAvdInsert", receivedMsg.getMsg(), null);
						ObjectOutputStream newoutputStream = new ObjectOutputStream(newSocket.getOutputStream());
						newoutputStream.writeObject(newmsgToSend);
						newoutputStream.flush();
						ObjectInputStream newinputStream = new ObjectInputStream(newSocket.getInputStream());
						MsgToSend newreceivedMsg = (MsgToSend) newinputStream.readObject();
						if (newreceivedMsg.getType().equals("InsertSucceed")) {
							Log.i("Insert3", "Succeeds");
						}
						newSocket.close();
					} catch (SocketException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
					MsgToSend msgReturn = SetMsgToSend("InsertSucceed", receivedMsg.getMsg(), null);
					objectOutputStream.writeObject(msgReturn);
					objectOutputStream.flush();

				} else if (receivedMsg.getType().equals("PreAvdInsert")||receivedMsg.getType().equals("PrePreAvdInsert")) {
					ContentValues contentValues = new ContentValues();
					String []seperated = receivedMsg.getMsg().split("\n");
					contentValues.put(KEY_FIELD, seperated[0]);
					contentValues.put(VALUE_FIELD, seperated[1]);
					if(receivedMsg.getType().equals("PreAvdInsert")){
						preContentValuesBase[precount++] = contentValues;
					}else if(receivedMsg.getType().equals("PrePreAvdInsert")){
						prepreContentValuesBase[preprecount++] = contentValues;
					}
					Log.i("pre&prepre countInsert", receivedMsg.getMsg()+" "+count+" inserted");
					MsgToSend msgReturn = SetMsgToSend("InsertSucceed", receivedMsg.getMsg(), null);
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
					objectOutputStream.writeObject(msgReturn);
					objectOutputStream.flush();

				} else if (receivedMsg.getType().equals("GiveMeYourmContent")) {
					String msg = "";
					for (int i = 0; i < count; i++) {
						msg=msg+  mContentValuesBase[i].getAsString(KEY_FIELD)+"\n"+mContentValuesBase[i].getAsString(VALUE_FIELD)+"\n";
					}
					MsgToSend msgToSend = SetMsgToSend(null,msg, null);
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
					objectOutputStream.writeObject(msgToSend);
					objectOutputStream.flush();
					//Log.i("GiveMePre&PrePre", "send out");
				} else if (receivedMsg.getType().equals("GiveMeYourPre")) {
					String msg = "";
					for (int i = 0; i < precount; i++) {
						msg+=  preContentValuesBase[i].getAsString(KEY_FIELD)+"\n"+preContentValuesBase[i].getAsString(VALUE_FIELD)+"\n";
					}

					MsgToSend msgToSend = SetMsgToSend(null, msg, null);
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
					objectOutputStream.writeObject(msgToSend);
					objectOutputStream.flush();
					//Log.i("GiveMeMine", "send out");

				} else if (receivedMsg.getType().equals("QueryAllFirstTrail") || receivedMsg.getType().equals("QueryAllSecondTrail")) {
					//and return to the socket.output query succeeds
					//check whether this is the destination
					//put mContentValues into the msg
					//and pass it to the next nodes
					//if next nodes fails, the pass it to the nextnext nodes
					MsgToSend msgToSend = SetMsgToSend("Iamalive", null, null);
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
							MsgToSend newmsgToSend = SetMsgToSend("QueryAllFirstTrail", msg, receivedMsg.getOriginal());
							ObjectOutputStream newobjectOutputStream = new ObjectOutputStream(newsocket.getOutputStream());
							newobjectOutputStream.writeObject(newmsgToSend);
							newobjectOutputStream.flush();
						} catch (UnknownHostException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}
						try {
							newsocket.setSoTimeout(2000);
							ObjectInputStream newinputStream = new ObjectInputStream(newsocket.getInputStream());
							MsgToSend receviedMsg = (MsgToSend) newinputStream.readObject();
							if (receviedMsg.getType().equals("Iamalive")) {
								Log.i("QueryFirst", "Iamalive");
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
								MsgToSend newmsgToSend = SetMsgToSend("QueryAllSecondTrail",  msg, receivedMsg.getOriginal());
								ObjectOutputStream newoutputStream = new ObjectOutputStream(newsocket.getOutputStream());
								newoutputStream.writeObject(newmsgToSend);
								newoutputStream.flush();

								ObjectInputStream newinputStream = new ObjectInputStream(newsocket.getInputStream());
								MsgToSend receviedMsg = (MsgToSend) newinputStream.readObject();
								if (receviedMsg.getType().equals("Iamalive")) {
									Log.i("QueryAll2", "Succeed");
								}
								newsocket.close();
								//Log.i("QueryAllSecondTrial", receviedMsg.getValue());
							} catch (UnknownHostException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							} catch (ClassNotFoundException e) {
								e.printStackTrace();
							}
						}
					}
				} else if(receivedMsg.getType().equals("QueryOneFirstTrail")||receivedMsg.getType().equals("QueryOneSecondTrail")){
					String selection = receivedMsg.getMsg();
					MsgToSend msgToSend = new MsgToSend();
					if(receivedMsg.getType().equals("QueryOneFirstTrail")){
						for(int i=0; i<count;i++){
							if(mContentValuesBase[i].getAsString(KEY_FIELD).equals(selection)){
								msgToSend = SetMsgToSend("Ifound",mContentValuesBase[i].getAsString(KEY_FIELD)+"\n"+mContentValuesBase[i].getAsString(VALUE_FIELD)+"\n",null);
								break;
							}
						}
					}else if(receivedMsg.getType().equals("QueryOneSecondTrail")){
						for(int i=0; i<precount;i++){
							if(preContentValuesBase[i].getAsString(KEY_FIELD).equals(selection)){
								msgToSend = SetMsgToSend("Ifound",preContentValuesBase[i].getAsString(KEY_FIELD)+"\n"+preContentValuesBase[i].getAsString(VALUE_FIELD)+"\n",null);
								break;
							}
						}
					}
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
					objectOutputStream.writeObject(msgToSend);
					objectOutputStream.flush();

				}else if (receivedMsg.getType().equals("DeleteAllFirstTrail") || receivedMsg.getType().equals("DeleteAllSecondTrail")) {
					MsgToSend msgToSend = SetMsgToSend("DeleteSucceed",  null, null);
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
							MsgToSend newmsgToSend = SetMsgToSend("DeleteAllFirstTrail", null,receivedMsg.getOriginal());
							ObjectOutputStream newobjectOutputStream = new ObjectOutputStream(newsocket.getOutputStream());
							newobjectOutputStream.writeObject(newmsgToSend);
							newobjectOutputStream.flush();
						} catch (UnknownHostException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}
						try {
							newsocket.setSoTimeout(2000);
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
								MsgToSend newmsgToSend = SetMsgToSend("DeleteAllSecondTrail",  null, receivedMsg.getOriginal());
								ObjectOutputStream newoutputStream = new ObjectOutputStream(newsocket.getOutputStream());
								newoutputStream.writeObject(newmsgToSend);
								newoutputStream.flush();

								ObjectInputStream newinputStream = new ObjectInputStream(newsocket.getInputStream());
								MsgToSend receviedMsg = (MsgToSend) newinputStream.readObject();
								if (receviedMsg.getType().equals("DeleteSucceed")) {
									Log.i("Delete", "Succeed");
								}
								newsocket.close();
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
							if (mContentValuesBase[i].getAsString(KEY_FIELD).equals(receivedMsg.getMsg())) {
								for (int j = i; j < count - 1; j++) {
									mContentValuesBase[j] = mContentValuesBase[j + 1];
								}
								count--;
							}
						}
					} else if (receivedMsg.getType().equals("DeleteOneSecondTrail")) {
						for (int i = 0; i < precount; i++) {
							if (preContentValuesBase[i].getAsString(KEY_FIELD).equals(receivedMsg.getMsg())) {
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
	public MsgToSend SetMsgToSend(String type, String msg, String original){
		MsgToSend msgToSend = new MsgToSend();
		msgToSend.settype(type);
		msgToSend.setMsg(msg);
		msgToSend.setOriginal(original);
		return msgToSend;
	}
	public  ContentValues[] SetContent(String msg){
		ContentValues [] tem = new ContentValues[TEST_CNT];
		for(int i=0; i<TEST_CNT; i++){
			tem[i] = new ContentValues();
		}
		String []seperated = msg.split("\n");
		if(msg.equals("")) return tem;
		for(int i=0; i<seperated.length;i+=2){
			int j = i/2;
			tem[j].put(KEY_FIELD, seperated[i]);
			tem[j].put(VALUE_FIELD,seperated[i+1]);
		}
		return  tem;
	}

}
