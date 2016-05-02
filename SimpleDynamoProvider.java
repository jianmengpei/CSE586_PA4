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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
	private static final int TEST_CNT = 100;
	private static int count = 0, precount = 0, preprecount = 0;
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	private static final int readtimeout = 500;
	final String[] matrix = {"key","value"};
	static final int server_port = 10000;
	private boolean queryall = false, queryone = false;
	private String queryallresult = null, queryoneresult = null;
	int pre=0, prepre=0, suc=0;
	private ReadWriteLock readWriteLock;
	private Lock readLock;
	private Lock writeLock;
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		Log.i("delete",selection);
		if(selection.equals("*")){
			for(int i=0; i<5; i++){
				if(avdNum[i].equals(selfAvdNum)){
					DeleteCon(3,null);
					continue;
				}
				SendDeleteto(avdNum[i],3,null);
			}
		}else if(selection.equals("@")){
			for (int i = 0; i < 5; i++) {
				if (avdNum[i].equals(selfAvdNum)) {
					DeleteCon(3,null);
					SendDeleteto(avdNum[(i+1)%5],4, null);
					SendDeleteto(avdNum[(i+2)%5],5, null);
					break;
				}
			}
		}else {
			int des = FindDes(selection);
			String []sendto = {avdNum[des],avdNum[(des+1)%5],avdNum[(des+2)%5]};
			for(int i=0; i<3; i++){
				if(sendto[i].equals(selfAvdNum)){
					DeleteCon(i+6,selection);
					continue;
				}
				SendDeleteto(sendto[i],i+6, selection);
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
		//1, genHash of the key of the values, and send it to the appropiate nodes if not owns
		int des = FindDes(values.getAsString(KEY_FIELD));

		String []sendto = {avdNum[des],avdNum[(des+1)%5],avdNum[(des+2)%5]};
		for(int i=0; i<3; i++){
			if(sendto[i].equals(selfAvdNum)){
				InsertToCon(i,values);
				continue;
			}
			SendConto(sendto[i], i, values);
		}
		return null;
	}
	public void SendConto(String des, int type, ContentValues values){
		try {
			Socket socket= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(des) * 2);
			ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
			MsgToSend msgToSend = SetMsgToSend(type, values.getAsString(KEY_FIELD)+"\n"+values.getAsString(VALUE_FIELD)+"\n",selfAvdNum);
			Log.i("sendto", values.getAsString(KEY_FIELD) + " and " + values.getAsString(VALUE_FIELD) + " send to " + des);
			outputStream.writeObject(msgToSend);
			outputStream.flush();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ;
	}
	public void SendDeleteto(String des, int type, String selection){
		try {
			Socket socket= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(des) * 2);
			ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
			MsgToSend msgToSend = SetMsgToSend(type, selection,selfAvdNum);
			Log.i("DeleteSendto",  selection + " send to " + des);
			outputStream.writeObject(msgToSend);
			outputStream.flush();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ;
	}
	public int FindDes(String string){
		int des = 0;
		try{
			String conKeyHash = genHash(string);
			for(int i=0; i<5; i++){
				if(avdHash[i].compareTo(conKeyHash)>=0){
					des = i;
					break;
				}
			}
			if(avdHash[4].compareTo(conKeyHash)<0) des = 0;
		}catch (NoSuchAlgorithmException e){
			e.printStackTrace();
		}
		return des;
	}
	public void InsertToCon(int i, ContentValues values){
		writeLock.lock();
		try {
			if (i == 0) {
				mContentValuesBase[count++] = values;
				Log.i("mContentInsert", values.getAsString(KEY_FIELD) + " " + values.getAsString(VALUE_FIELD) + " " + (count - 1) + " inserted");
			} else if (i == 1) {
				preContentValuesBase[precount++] = values;
				Log.i("preContentInsert", values.getAsString(KEY_FIELD) + " " + values.getAsString(VALUE_FIELD) + " " + (precount - 1) + " inserted");
			} else if (i == 2) {
				prepreContentValuesBase[preprecount++] = values;
				Log.i("prepreContentInsert", values.getAsString(KEY_FIELD) + " " + values.getAsString(VALUE_FIELD) + " " + (preprecount - 1) + " inserted");
			}
		} finally {
			writeLock.unlock();
		}
		return ;
	}
	public void DeleteCon(int type, String key){
		writeLock.lock();
		try {
			switch (type){
				case 3: //delete all
					writeLock.lock();
					try {
						count = 0;
						precount = 0;
						preprecount = 0;
					}finally {
						writeLock.unlock();
					}
					Log.i("AllMyDeletedm", "Succeeds");
					break;
				case 4://delete preContent
					writeLock.lock();
					try {
						precount = 0;
					}finally {
						writeLock.unlock();
					}
					Log.i("AllMyDeletedPre", "Succeeds");
					break;
				case 5://delete prepreContent
					writeLock.lock();
					try {
						preprecount = 0;
					}finally {
						writeLock.unlock();
					}
					Log.i("AllMyDeletedPrePre", "Succeeds");
					break;
				case 6:
					//delete key in mContent
					for (int i = 0; i < count; i++) {
						if (mContentValuesBase[i].getAsString(KEY_FIELD).equals(key)) {
							for (int j = i; j < count - 1; j++) {
								mContentValuesBase[j] = mContentValuesBase[j + 1];
							}
							count--;
						}
					}
					break;
				case 7:
					//delete key in preContent
					for (int i = 0; i < precount; i++) {
						if (preContentValuesBase[i].getAsString(KEY_FIELD).equals(key)) {
							for (int j = i; j < precount - 1; j++) {
								preContentValuesBase[j] = preContentValuesBase[j + 1];
							}
							precount--;
						}
					}
					break;
				case 8:
					//delete key in prepreContent
					for (int i = 0; i < preprecount; i++) {
						if (prepreContentValuesBase[i].getAsString(KEY_FIELD).equals(key)) {
							for (int j = i; j < preprecount - 1; j++) {
								prepreContentValuesBase[j] = prepreContentValuesBase[j + 1];
							}
							preprecount--;
						}
					}
					break;
			}
		} finally {
			writeLock.unlock();
		}
		return ;
	}
	public MsgToSend SetMsgToSend(int type, String msg, String original){
		MsgToSend msgToSend = new MsgToSend();
		msgToSend.settype(type);
		msgToSend.setMsg(msg);
		msgToSend.setOriginal(original);
		return msgToSend;
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

		readWriteLock = new ReentrantReadWriteLock();
		readLock = readWriteLock.readLock();
		writeLock = readWriteLock.writeLock();

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
		Log.i("Query",selection);
		MatrixCursor cs = new MatrixCursor(matrix, 1);
		queryoneresult = null;
		queryallresult = "";

		if(selection.equals("*")){
			String singleresult = null;
			for(int i = 0; i<5; i++){
				if(avdNum[i].equals(selfAvdNum)){
					String msg = "";
					readLock.lock();
					try {
						for (int j = 0; j < count; j++) {
							msg = msg + mContentValuesBase[j].getAsString(KEY_FIELD) + "\n" + mContentValuesBase[j].getAsString(VALUE_FIELD) + "\n";
						}
					}finally {
						readLock.unlock();
					}
					singleresult = msg;
				}else {
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[i]) * 2);
						ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
						MsgToSend msgToSend = SetMsgToSend(9, selection, selfAvdNum);
						outputStream.writeObject(msgToSend);
						outputStream.flush();
						ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
						MsgToSend receivedMsg = (MsgToSend) inputStream.readObject();
						singleresult = receivedMsg.getMsg();
						Log.i("QueryAllFrom", receivedMsg.getOriginal() + " " + singleresult);
					} catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (IOException e) {
						i++;
						if(avdNum[i].equals(selfAvdNum)){
							String msg = "";
							readLock.lock();
							try {
								for (int j = 0; j < count; j++) {
									msg = msg + mContentValuesBase[j].getAsString(KEY_FIELD) + "\n" + mContentValuesBase[j].getAsString(VALUE_FIELD) + "\n";
								}
							}finally {
								readLock.unlock();
							}
							singleresult = msg;
						}else {
							try {
								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdNum[i]) * 2);
								ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
								MsgToSend msgToSend = SetMsgToSend(10, selection, selfAvdNum);
								outputStream.writeObject(msgToSend);
								outputStream.flush();
								ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
								MsgToSend receivedMsg = (MsgToSend) inputStream.readObject();
								singleresult = receivedMsg.getMsg();
								Log.i("QueryAllFrom2", receivedMsg.getOriginal() + " " + singleresult);
							} catch (UnknownHostException e1) {
								e1.printStackTrace();
							} catch (IOException e1) {
								e1.printStackTrace();
							} catch (ClassNotFoundException e1) {
								e1.printStackTrace();
							}
						}
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}
				}
				queryallresult += singleresult;
			}
			String []seperated = queryallresult.split("\n");
			for (int i = 0; i < seperated.length; i += 2) {
				cs.addRow(new Object[]{seperated[i], seperated[i + 1]});
				Log.i("queryall", seperated[i] + " " + seperated[i + 1]);
			}
		}else if(selection.equals("@")){
			readLock.lock();
			try {
				for (int i = 0; i < count; i++) {
					cs.addRow(new Object[]{mContentValuesBase[i].getAsString(KEY_FIELD), mContentValuesBase[i].getAsString(VALUE_FIELD)});
				}
				for (int i = 0; i < precount; i++) {
					cs.addRow(new Object[]{preContentValuesBase[i].getAsString(KEY_FIELD), preContentValuesBase[i].getAsString(VALUE_FIELD)});
				}
				for (int i = 0; i < preprecount; i++) {
					cs.addRow(new Object[]{prepreContentValuesBase[i].getAsString(KEY_FIELD), prepreContentValuesBase[i].getAsString(VALUE_FIELD)});
				}
			}finally {
				readLock.unlock();
			}
		}else{
			//Log.i("QueryOne","Starts...");
			int des = FindDes(selection);
			String []sendto = {avdNum[des],avdNum[(des+1)%5],avdNum[(des+2)%5]};
			String singleresult = null;
			for(int i=0; i<3; i++){
				try {
					Socket socket= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(sendto[i]) * 2);
					ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
					MsgToSend msgToSend = SetMsgToSend(11+i, selection,selfAvdNum);
					outputStream.writeObject(msgToSend);
					outputStream.flush();
					ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
					MsgToSend receivedMsg = (MsgToSend)inputStream.readObject();
					singleresult = receivedMsg.getMsg();
					Log.i("QueryOneFrom",receivedMsg.getOriginal()+" " +singleresult);
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					try {
						Socket socket= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(sendto[++i]) * 2);
						ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
						MsgToSend msgToSend = SetMsgToSend(11+i, selection,selfAvdNum);
						outputStream.writeObject(msgToSend);
						outputStream.flush();
						ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
						MsgToSend receivedMsg = (MsgToSend)inputStream.readObject();
						singleresult = receivedMsg.getMsg();
						Log.i("QueryOneFrom2",receivedMsg.getOriginal()+" " +singleresult);
					} catch (UnknownHostException e1) {
						e1.printStackTrace();
					} catch (IOException e1) {
						e1.printStackTrace();
					}catch (ClassNotFoundException e1){
						e1.printStackTrace();
					}
					e.printStackTrace();
				}catch (ClassNotFoundException e){
					e.printStackTrace();
				}
				if (singleresult != null) {
					writeLock.lock();
					try {
						queryoneresult = singleresult;
					}finally {
						writeLock.unlock();
					}
					break;
				}
			}
			String []seperated = queryoneresult.split("\n");
			cs.addRow(new Object[]{seperated[0], seperated[1]});

			Log.i("queryone2", seperated[0] + " " + seperated[1]);
		}
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
			String []sendto = {null,null,null};
			for(int i=0; i<5; i++){
				if(avdNum[i].equals(selfAvdNum)){
					sendto[0] = avdNum[(i+5-2)%5];
					sendto[1] = avdNum[(i+5-1)%5];
					sendto[2] = avdNum[(i+1)%5];
					break;
				}
			}
			try {
				for(int i = 0; i<3; i++) {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(sendto[i]) * 2);
					//Log.i("port number pre","is "+ Integer.parseInt(avdNum[pre]) * 2);
					MsgToSend msgToSend = SetMsgToSend(14+i, null, selfAvdNum);
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
					objectOutputStream.writeObject(msgToSend);
					objectOutputStream.flush();

					ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
					MsgToSend receivedMsg = (MsgToSend) objectInputStream.readObject();
					writeLock.lock();
					try {
						if(receivedMsg.getMsg()!=null && (!receivedMsg.getMsg().equals(""))){
							String[] seperated = receivedMsg.getMsg().split("\n");
							switch (i){
								case 0:
									preprecount = seperated.length / 2;
									for (int j = 0; j < seperated.length; j += 2) {
										prepreContentValuesBase[(j/2)].put(KEY_FIELD, seperated[j]);
										prepreContentValuesBase[(j/2)].put(VALUE_FIELD, seperated[j + 1]);
										Log.i("PrepreReFrom", seperated[j]+" "+seperated[j+1]+" from "+receivedMsg.getOriginal());
									}
									break;
								case 1:
									precount = seperated.length / 2;
									for (int j = 0; j < seperated.length; j += 2) {
										preContentValuesBase[(j/2)].put(KEY_FIELD, seperated[j]);
										preContentValuesBase[(j/2)].put(VALUE_FIELD, seperated[j + 1]);
										Log.i("PreReFrom", seperated[j] + " " + seperated[j+1] + " from " + receivedMsg.getOriginal());
									}
									break;
								case 2:
									count = seperated.length / 2;
									for (int j = 0; j < seperated.length; j += 2) {
										mContentValuesBase[(j/2)].put(KEY_FIELD, seperated[j]);
										mContentValuesBase[(j/2)].put(VALUE_FIELD, seperated[j + 1]);
										Log.i("MReFrom", seperated[j] + " " + seperated[j + 1] + " from " + receivedMsg.getOriginal());
									}
									break;
							}
						}
					} finally {
						writeLock.unlock();
					}
					socket.close();
				}
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
			try {
				ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
				MsgToSend receivedMsg = (MsgToSend) objectInputStream.readObject();
				//Log.i("type and avd", receivedMsg.getType()+receivedMsg.getOriginal());
				String msg = "";
				MsgToSend msgToSend = null;
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
				switch (receivedMsg.getType()){

					case 0://insert to mContent
					case 1://insert to preContent
					case 2://insert to prepreContent
						String []seperated = receivedMsg.getMsg().split("\n");
						ContentValues contentValues = new ContentValues();
						contentValues.put(KEY_FIELD,seperated[0]);
						contentValues.put(VALUE_FIELD,seperated[1]);
						InsertToCon(receivedMsg.getType(), contentValues);
						break;
					case 3:
					case 4:
					case 5:
					case 6:
					case 7:
					case 8:
						DeleteCon(receivedMsg.getType(),receivedMsg.getMsg());
						break;
					case 9:
						//send mContent
						readLock.lock();
						try {
							for (int i = 0; i < count; i++) {
								msg = msg + mContentValuesBase[i].getAsString(KEY_FIELD) + "\n" + mContentValuesBase[i].getAsString(VALUE_FIELD) + "\n";
							}
						}finally {
							readLock.unlock();
						}
						msgToSend = SetMsgToSend(9,msg, selfAvdNum);
						objectOutputStream.writeObject(msgToSend);
						objectOutputStream.flush();
						break;
					case 10:
						//send preContent
						readLock.lock();
						try {
							for (int i = 0; i < precount; i++) {
								msg = msg + preContentValuesBase[i].getAsString(KEY_FIELD) + "\n" + preContentValuesBase[i].getAsString(VALUE_FIELD) + "\n";
							}
						}finally {
							readLock.unlock();
						}
						msgToSend = SetMsgToSend(10,msg, selfAvdNum);
						objectOutputStream.writeObject(msgToSend);
						objectOutputStream.flush();
						break;
					case 11:
						//query in the mContent
						readLock.lock();
						try {
							for (int i = 0; i < count; i++) {
								if (mContentValuesBase[i].getAsString(KEY_FIELD).equals(receivedMsg.getMsg())) {
									msgToSend = SetMsgToSend(11, mContentValuesBase[i].getAsString(KEY_FIELD) + "\n" + mContentValuesBase[i].getAsString(VALUE_FIELD) + "\n", selfAvdNum);
									break;
								}
							}
						}finally {
							readLock.unlock();
						}
						objectOutputStream.writeObject(msgToSend);
						objectOutputStream.flush();
						break;
					case 12:
						//query in the preContent
						readLock.lock();
						try{
							for (int i = 0; i < precount; i++) {
								if (preContentValuesBase[i].getAsString(KEY_FIELD).equals(receivedMsg.getMsg())) {
									msgToSend = SetMsgToSend(12, preContentValuesBase[i].getAsString(KEY_FIELD) + "\n" + preContentValuesBase[i].getAsString(VALUE_FIELD) + "\n", selfAvdNum);
									break;
								}
							}
						}finally {
							readLock.unlock();
						}
						objectOutputStream.writeObject(msgToSend);
						objectOutputStream.flush();
						break;
					case 13:
						//query in the prepreContent
						readLock.lock();
						try{
							for (int i = 0; i < preprecount; i++) {
								if (prepreContentValuesBase[i].getAsString(KEY_FIELD).equals(receivedMsg.getMsg())) {
									msgToSend = SetMsgToSend(12, prepreContentValuesBase[i].getAsString(KEY_FIELD) + "\n" + prepreContentValuesBase[i].getAsString(VALUE_FIELD) + "\n", selfAvdNum);
									break;
								}
							}
						}finally {
							readLock.unlock();
						}
						objectOutputStream.writeObject(msgToSend);
						objectOutputStream.flush();
						break;
					case 14:
					case 15:
						readLock.lock();
						try {
							for (int i = 0; i < count; i++) {
								msg = msg + mContentValuesBase[i].getAsString(KEY_FIELD) + "\n" + mContentValuesBase[i].getAsString(VALUE_FIELD) + "\n";
							}
						}finally {
							readLock.unlock();
						}
						msgToSend = SetMsgToSend(9,msg, selfAvdNum);
						objectOutputStream.writeObject(msgToSend);
						objectOutputStream.flush();
						break;
					case 16:
						readLock.lock();
						try {
							for (int i = 0; i < precount; i++) {
								msg = msg + preContentValuesBase[i].getAsString(KEY_FIELD) + "\n" + preContentValuesBase[i].getAsString(VALUE_FIELD) + "\n";
								Log.i("SendpreContto", preContentValuesBase[i].getAsString(KEY_FIELD) + " " + preContentValuesBase[i].getAsString(VALUE_FIELD)+" to " +receivedMsg.getOriginal());
							}
						}finally {
							readLock.unlock();
						}
						msgToSend = SetMsgToSend(9,msg, selfAvdNum);
						objectOutputStream.writeObject(msgToSend);
						objectOutputStream.flush();
						break;
					default:
						break;

				}
			}catch (IOException e){
				e.printStackTrace();
			}catch (ClassNotFoundException e){
				e.printStackTrace();
			}
		}
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