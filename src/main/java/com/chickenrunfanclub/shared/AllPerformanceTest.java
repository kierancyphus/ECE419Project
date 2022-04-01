package com.chickenrunfanclub.shared;


import com.chickenrunfanclub.app_kvECS.ECSClient;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class AllPerformanceTest {
    private static final Logger logger = Logger.getRootLogger();
    private static final String host = "localhost";
    private static final int port = 50180;
    private final double putRatio;
    private final int numServers;
    private final int numClients;
    private final ArrayList<ClientThreadUtil> popClients;
    private final ArrayList<ClientThreadUtil> clients;
    private final ArrayList<ArrayList<String>> data;
    private ECSClient ecs;

    public AllPerformanceTest(String config, int maxReq, String dataPath, int numServers, int numClients, double putRatio) {
        this.putRatio = putRatio;
        this.numServers = numServers;
        this.numClients = numClients;

        clients = new ArrayList<>();
        popClients = new ArrayList<>();
        data = new ArrayList<>();
        fillData(dataPath, dataPath, data);
        try {
            ecs = new ECSClient(config, "LRU", 100, port);
            ecs.addNodes(numServers);
        } catch (Exception e) {
            e.printStackTrace();
        }

        int reqPerClient = Math.min(maxReq, data.size() / numClients);

        for (int i = 0; i < numClients; i++) {
            popClients.add(new ClientThreadUtil(config, data.subList(i * reqPerClient,
                    (i + 1) * reqPerClient), true, 1));
            clients.add(new ClientThreadUtil(config, data.subList(i * reqPerClient,
                    (i + 1) * reqPerClient), false, putRatio));
        }
        runClientBatch(popClients);

    }


    public void runClientBatch(ArrayList<ClientThreadUtil> clients) {
        ArrayList<Thread> allThreads = new ArrayList<>();
        for (ClientThreadUtil cl : clients) {
            Thread t = new Thread(cl);
            t.start();
            allThreads.add(t);
        }

        for (Thread t : allThreads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    public void fillData(String currentDir, String rootDir, ArrayList<ArrayList<String>> dat) {
        File dir = new File(currentDir);
        for (File temp : dir.listFiles()) {
            if (temp.isDirectory()) {
                fillData(temp.getPath(), rootDir, data);
            } else {
                try {
                    String value = new String(Files.readAllBytes(temp.toPath()));
                    if (value.length() > 2000) {
                        value = value.substring(0, 2000);
                    }
                    String key = temp.getPath();
                    if (key.length() > 30) {
                        key = key.substring(0, 30);
                    }
                    ArrayList<String> pair = new ArrayList<>(List.of(key, value));
                    data.add(pair);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void runEvaluation() {
        runClientBatch(clients);
        int getCounter = 0;
        int putCounter = 0;
        double putTime = 0.0;
        double getTime = 0.0;
        for (ClientThreadUtil cl : clients) {
            getCounter += cl.getCounter;
            putCounter += cl.putCounter;
            putTime += cl.putTime;
            getTime += cl.getTime;
        }

        System.out.println(numServers);
        System.out.println(numClients);
        System.out.println(putRatio);
        System.out.println(putTime + getTime);
        System.out.println(putCounter + getCounter);
        System.out.println(putTime / putCounter);
        System.out.println(getTime / getCounter);

        try {
            ecs.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
