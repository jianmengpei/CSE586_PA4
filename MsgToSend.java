package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.telephony.TelephonyManager;

import java.io.Serializable;

/**
 * Created by pjm on 4/7/16.
 */
public class MsgToSend implements Serializable{

    private String original = null;
    private String selfavd = null;
    private static int count = 0;
    private static final int TEST_CNT = 50;
    private String msg = null;
    private String type = null;
    private String key=null;
    private String value = null;
    private String dest = null;
    private String successor = null, predecessor = null;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private String [] keys;
    private String [] values;
    void MsgToSend(){

    }

    public void setOriginal(String original){
        this.original = original;
    }
    public void setCount(int count){
        this.count = count;
    }
    public  void setSelfavd(String selfavd){
        this.selfavd = selfavd;
    }
    public  void setMsg(String msg){
        this.msg = msg;
    }
    public void settype(String type){
        this.type = type;
    }
    public void setkey(String key){
        this.key = key;
    }
    public void setvalue(String value){
        this.value = value;
    }
    public  void setDest(String dest){
        this.dest = dest;
    }
    public  void setSuccessor(String successor){
        this.successor = successor;
    }
    public void setPredecessor(String predecessor){
        this.predecessor = predecessor;
    }
    public void setKeys(String [] keys){
        this.keys = keys;
    }
    public void setValues(String [] values){
        this.values = values;
    }
    public String getOriginal(){
        return original;
    }
    public int getCount(){
        return count;
    }
    public String getMsg(){
        return msg;
    }
    public String getType(){
        return type;
    }
    public String getKey(){
        return key;
    }
    public String getValue(){
        return value;
    }
    public String getDest(){
        return dest;
    }
    public String getSuccessor(){
        return successor;
    }
    public String getPredecessor(){
        return predecessor;
    }
    public String getSelfavd(){
        return selfavd;
    }
    public String []getKeys(){
        return keys;
    }
    public String []getValues(){
        return values;
    }
}
