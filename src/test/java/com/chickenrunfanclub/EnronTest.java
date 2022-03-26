package com.chickenrunfanclub;

import com.chickenrunfanclub.app_kvECS.ECSClient;
import com.chickenrunfanclub.client.KVStore;
import com.chickenrunfanclub.shared.ClientThreadUtil;
import junit.framework.TestCase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
public class EnronTest extends TestCase {

    private final int NUM_SERVERS = 5;
    private final int NUM_CLIENT = 100;
    private final int CACHE_SIZE = 100;
    private final String CACHE_STRATEGY = "None";
    private final int port = 50000;
    private final String DATAPATH = "/Users/rui/Downloads/maildir/wolfe-j/all_documents/";

    private ECSClient ecsClient;

    @BeforeAll
    public void setUp() {
        try {
            ecsClient = new ECSClient("ecs.config", CACHE_STRATEGY, CACHE_SIZE, 50000);
            ecsClient.addNodes(NUM_SERVERS);
            ecsClient.start();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    @AfterAll
    public void tearDown() {
        try{
            ecsClient.shutdown();
        }catch(Exception e){
            e.printStackTrace();
        }

    }

    @Test
    public void test_none() {
        try {
            File file = new File(DATAPATH);
            HashMap<String, String> data = getData(file);
            KVStore client;
            for (int c_num = 5; c_num < NUM_CLIENT; c_num += 5) {
                ArrayList<KVStore> KVClients = new ArrayList<>();
                CountDownLatch latch = new CountDownLatch(KVClients.size());
                for (int i = 0; i < c_num; i++) {
                    client = new KVStore("localhost", 50012);
                    client.connect();
                    KVClients.add(client);
                }
                long start = System.currentTimeMillis();
                for (KVStore ref : KVClients) {
                    ClientThreadUtil ct = new ClientThreadUtil(data, latch, ref);
                    new Thread(ct).start();
                }
                try{
                    latch.await();
                }catch (Exception e){
                    e.printStackTrace();
                }
                long end = System.currentTimeMillis();
                System.out.println("Processing: " + (end - start) + "ms");
                System.out.println();

            }
        } catch (Exception e) {
            e.printStackTrace();

        }
    }

    private HashMap<String, String> getData(File file) {
        HashMap<String, String> map = new HashMap<>();
        for (File temp : file.listFiles()) {
            try {
                String value = new String(Files.readAllBytes(temp.toPath()));
                if(value.length() > 2000){
                    value = value.substring(0,2000);
                }
                String key = temp.getPath();
                if(key.length() > 30){
                    key = key.substring(0,30);
                }
                map.put(key, value);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return map;
    }
}