package com.chickenrunfanclub.shared;
import com.chickenrunfanclub.client.KVStore;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

public class ClientThreadUtil implements Runnable {
    private HashMap<String,String> map;
    private  CountDownLatch latch;
    private KVStore client;

    public ClientThreadUtil(HashMap<String,String> map, CountDownLatch latch, KVStore client){
        this.client = client;
        this.latch = latch;
        this.map = map;
    }

    public void run(){
        for (String i: map.keySet()) {
            try{
                client.put(i, map.get(i));
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        for (String i: map.keySet()) {
            try{
                client.get(i);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        latch.countDown();
    }
}