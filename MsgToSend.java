package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.telephony.TelephonyManager;

import java.io.Serializable;

/**
 * Created by pjm on 4/7/16.
 */
public class MsgToSend implements java.io.Serializable{

    private String original = null;
    private String msg = null;
    private int type = 0;
    void MsgToSend(){

    }
    public void setOriginal(String original){
        this.original = original;
    }
    public  void setMsg(String msg){
        this.msg = msg;
    }
    public void settype(int type){
        this.type = type;
    }

    public String getOriginal(){
        return original;
    }
    public String getMsg(){
        return msg;
    }
    public int getType(){
        return type;
    }
}