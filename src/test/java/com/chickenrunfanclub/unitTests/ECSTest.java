package com.chickenrunfanclub.unitTests;

import com.chickenrunfanclub.TestUtils;
import com.chickenrunfanclub.app_kvECS.ECSClient;
import com.chickenrunfanclub.app_kvServer.KVServer;
import com.chickenrunfanclub.client.KVStore;
import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.shared.messages.IKVMessage;
import com.google.gson.Gson;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class ECSTest {
    private static ECSClient ecs;
    private KVStore client;

    @AfterAll
    public static void shutdown(){
        try{
            ecs.shutdown();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void testECS(){

        Runtime run = Runtime.getRuntime();
        String filepath = "./src/test/resources/servers.cfg";
        try{

            ecs = new ECSClient(filepath, "FIFO", 50);
            ecs.addNode("FIFO", 50);
            ecs.addNode("FIFO", 50);
            ecs.addNode("FIFO", 50);
            ecs.start();

        } catch (Exception e){
            e.printStackTrace();

        }

    }
}

