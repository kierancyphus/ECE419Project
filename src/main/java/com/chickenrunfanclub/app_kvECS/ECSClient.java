package com.chickenrunfanclub.app_kvECS;

import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.ecs.ECSNode.ECSNodeFlag;
import com.chickenrunfanclub.ecs.IECSNode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import java.security.NoSuchAlgorithmException;

public class ECSClient implements IECSClient {
    private static Logger logger = LogManager.getLogger(ECSClient.class);
    private ZooKeeper zk;
    private ArrayList<ECSNode> nodeList = new ArrayList<ECSNode>();
    private HashMap<String, String> metaData = new HashMap<String, String>();   // node to hash in use
    private HashMap<String, String> hashToName = new HashMap<String, String>();     // hash to node
    private HashMap<String, IECSNode> nameToNode = new HashMap<String, IECSNode>();     // hash to node
    private HashMap<String, ECSNodeFlag> serverStatus = new HashMap<String, ECSNodeFlag>();     // node status
    private CountDownLatch connectedSignal;
    private static final String SCRIPT_TEXT = "ssh -n %s nohup java -jar ./m2-server.jar %s %s %s &";
    private final int TIMEOUT = 15000;
    private int numServers = 0;
    private String cacheStrategy;
    private int cacheSize;
    private boolean running = false;

    public ECSClient(String configFileName, String cacheStrat, int cacheSiz) throws IOException, InterruptedException, KeeperException {
        // start zookeeper connection
        connectedSignal = new CountDownLatch(1);
        cacheStrategy = cacheStrat;
        cacheSize = cacheSiz;
        zk = new ZooKeeper("127.0.0.1",5000,new Watcher() {
            public void process(WatchedEvent we) { if (we.getState() == KeeperState.SyncConnected) {
                connectedSignal.countDown();
                } } });

        zk.create("/ecs", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/ecs/metadata", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        // load all ECS nodes from config file
        readFile(configFileName);
    }

    @Override
    public boolean start() throws InterruptedException, KeeperException {
        try {
            List<String> nodes = zk.getChildren("/ecs", false);
            for (String node : nodes) {
                if (!node.equals("metadata")) {
                    serverStatus.put(node, ECSNodeFlag.IN_USE);
                    metaData.put(node, hash(node));
                }
            }
            running = true;
            return true;
        } catch (Exception e) {
            System.out.println("Unable to start ECS.");
            logger.error("Unable to start ECS. " + e);
        }
        return false;
    }

    @Override
    public boolean stop() throws InterruptedException, KeeperException {
        List<String> nodes = zk.getChildren("/ecs", false);
        for (String node : nodes){
            if (!node.equals("metadata")) {
                serverStatus.put(node, ECSNodeFlag.STOP);
            }
        }
        running = false;
        return true;
    }

    @Override
    public boolean shutdown() throws InterruptedException, KeeperException {
        List<String> nodes = zk.getChildren("/ecs", false);
        for (String node : nodes){
            if (!node.equals("metadata")) {
                serverStatus.put(node, ECSNodeFlag.SHUT_DOWN);
                zk.delete("/ecs/" + node, -1);
            }
        }
        zk.setData("/ecs/metadata", null, -1);
        zk.delete("/ecs/metadata", -1);
        zk.delete("/ecs", -1);
        zk.close();
        return true;
    }

    @Override
    public IECSNode addNode() throws InterruptedException, KeeperException {
        try{
            // check for available node first
            if (metaData.size() >= nodeList.size()){
                System.out.println("add node unsuccessful due to insufficient nodes.");
                logger.error("add node unsuccessful due to insufficient nodes.");
                return null;
            }
            ECSNode node = null;
            Set<String> nodesInUse = metaData.keySet();
            // get the list of nodes we are setting up, update metadata and status
            for (ECSNode n : nodeList){
                if (!nodesInUse.contains(n.getNodeName())
                        && (serverStatus.get(n.getNodeName()) == ECSNodeFlag.START
                        || serverStatus.get(n.getNodeName()) == ECSNodeFlag.IDLE)){
                    node = n;
                    node.setCacheSize(cacheSize);
                    node.setCacheStrategy(cacheStrategy);
                    node.setHashStart(null);
                    nameToNode.put(node.getNodeName(), node);
                    metaData.put(node.getNodeName(), node.getServerHashVal());
                    numServers++;
                    serverStatus.put(node.getNodeName(), ECSNodeFlag.IN_USE);
                    // create zookeeper nodes
                    zk.create("/ecs/" + node.getNodeName(), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    break;
                }
            }
            // writeMetaData();

            Runtime run=Runtime.getRuntime();
            String script = String.format(SCRIPT_TEXT, node.getNodeHost(), node.getNodePort(),
                    node.getCachesize(), node.getCacheStrategy());
            Process proc= run.exec(script);
            proc.waitFor();
        } catch (Exception e) {
            logger.error("can not add nodes " + e);
        }
        return null;
    }

    @Override
    public ArrayList<ECSNode> addNodes(int count) throws InterruptedException {
        try{
            ArrayList<ECSNode> newNodes = setupNodes(count, cacheStrategy, cacheSize);
            Runtime run=Runtime.getRuntime();
            for(ECSNode node: newNodes) {
                String script = String.format(SCRIPT_TEXT, node.getNodeHost(), node.getNodePort(),
                        node.getCachesize(), node.getCacheStrategy());
                Process proc = run.exec(script);
                proc.waitFor();
            }
            if (awaitNodes(newNodes.size(), TIMEOUT)) {
                return newNodes;
            }
        } catch (Exception e) {
            logger.error("can not add nodes " + e);
        }
        return null;
    }

    @Override
    public ArrayList<ECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // check for available node first
        if (metaData.size() + count > nodeList.size()){
            System.out.println("set up nodes unsuccessful due to insufficient nodes.");
            logger.error("set up nodes unsuccessful due to insufficient nodes.");
            return null;
        }
        ArrayList<ECSNode> nodes = new ArrayList<>();
        Set<String> nodesInUse = metaData.keySet();
        int counter = 0;
        try {
            // get the list of nodes we are setting up, update metadata and status
            for (ECSNode node : nodeList){
                if (!nodesInUse.contains(node.getNodeName())
                        && (serverStatus.get(node.getNodeName()) == ECSNodeFlag.START
                        || serverStatus.get(node.getNodeName()) == ECSNodeFlag.IDLE)){
                    node.setCacheSize(cacheSize);
                    node.setCacheStrategy(cacheStrategy);
                    node.setHashStart(null);
                    nodes.add(node);
                    nameToNode.put(node.getNodeName(), node);
                    // metaData.put(node.getNodeName(), node.getServerHashVal());
                    numServers++;
                    serverStatus.put(node.getNodeName(), ECSNodeFlag.STOP);
                    // create zookeeper nodes
                    zk.create("/ecs/" + node.getNodeName(), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                    counter++;
                    if (counter == count){
                        break;
                    }
                }
            }
            // writeMetaData();
            return getNodeStartHashVal(nodes);
        }
        catch (Exception e){
            logger.error("Set up nodes " + e);
        }
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        long startTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - startTime < timeout) {
            List<String> list = zk.getChildren("/ecs", true);
            if (numServers + 1 == list.size()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) throws Exception {
        try {
            for (String node : nodeNames) {
                serverStatus.put(node, ECSNodeFlag.SHUT_DOWN);
                metaData.remove(node);
                zk.setData("/ecs/"+node,null,-1);
                zk.delete("/ecs/"+node, -1);
            }
            writeMetaData();
            return true;
        } catch (KeeperException | InterruptedException | NoSuchAlgorithmException  e) {
            logger.error(e);
            return false;
        }
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        HashMap<String, IECSNode> map = new HashMap<String, IECSNode>();
        for (String key : nameToNode.keySet()) {
            if (serverStatus.get(key) == ECSNodeFlag.IN_USE) {
                map.put(key, nameToNode.get(key));
            }
        }
        // now need to organize the hash ranges for these nodes
        String start, max, end, temp;
        IECSNode node;
        for (String key : map.keySet()) {
            node = map.get(key);
            start = "";
            max = "";
            end = node.getNodeHashRange()[1];
            for (IECSNode n : map.values()){
                temp = n.getNodeHashRange()[1];
                if (temp.compareTo(end) < 0 && temp.compareTo(start) > 0){
                    start = temp;
                }
                else if (temp.compareTo(max) > 0) {
                    max = temp;
                }
            }
            if (!start.equals("")) {
                node.setHashStart(start);
            }
            else {
                node.setHashStart(max);
            }
            map.put(key, node);
        }
        return map;
    }

    @Override
    public IECSNode getNodeByKey(String Key) throws UnsupportedEncodingException, NoSuchAlgorithmException {
        String hashedKey = hash(Key);
        ArrayList<IECSNode> nodes = (ArrayList<IECSNode>) getNodes().values();
        if (nodes.size() == 1){
            return nodes.get(0);
        }
        IECSNode closest = null;
        IECSNode min = null;
        for (IECSNode node : nodes) {
            if (hashedKey.compareTo(node.getNodeHashRange()[1]) < 0) {
                if (closest == null) {
                    closest = node;
                }
                else if (closest.getNodeHashRange()[1].compareTo(node.getNodeHashRange()[1]) > 0) {
                    closest = node;
                }
            }
            if (closest == null){
                if (min == null){
                    min = node;
                }
                else if (min.getNodeHashRange()[1].compareTo(node.getNodeHashRange()[1]) > 0) {
                    min = node;
                }
            }
        }
        if (closest != null){
            return closest;
        }
        return min;
    }

    public boolean isRunning() {
        return running;
    }

    // MD5 hashing
    public String hash(String key) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte [] hashed = md.digest(key.getBytes("UTF-8"));
        StringBuilder sb = new StringBuilder();
        for (byte b : hashed) {
            sb.append(String.format("%02X ", b));
        }
        return sb.toString();
    }

    private void readFile(String file){
        File f = new File(file);
        String name, host, key, port, hashVal;
        ECSNode node;
        try {
            Scanner scanner = new Scanner(f);
            while (scanner.hasNextLine()) {
                String[] line = scanner.nextLine().split(" ");
                name = line[0];
                host = line[1];
                port = line[2];
                key = host + ":" + port;
                hashVal = hash(key);
                node = new ECSNode(name, host, Integer.parseInt(port), hashVal);
                nodeList.add(node);
                hashToName.put(hashVal, name);
            }
        } catch (FileNotFoundException e) {
            System.out.println("Configuration file not found");
            e.printStackTrace();
            System.exit(1);
        } catch (UnsupportedEncodingException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    private byte[] HashMapToByte(HashMap<String, String> map) throws IOException {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(byteOut);
        out.writeObject(map);
        return byteOut.toByteArray();
    }

    private HashMap<String,String> ByteToHashMap(byte[] byteArray) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteIn = new ByteArrayInputStream(byteArray);
        ObjectInputStream in = new ObjectInputStream(byteIn);
        return (HashMap<String,String>)in.readObject();
    }

    private void writeMetaData() throws Exception{
        zk.setData("/servers/metadata",HashMapToByte(metaData), -1);
    }

    // calculate the start hash value for nodes
    private ArrayList<ECSNode> getNodeStartHashVal(ArrayList<ECSNode> nodes){
        ArrayList<ECSNode> out = new ArrayList<ECSNode>();
        String start, end, temp, max;
        for (ECSNode node : nodes) {
            start = "";
            max = "";
            end = node.getNodeHashRange()[1];
            for (ECSNode n : nodes){
                temp = n.getNodeHashRange()[1];
                if (temp.compareTo(end) < 0 && temp.compareTo(start) > 0){
                    start = temp;
                }
                else if (temp.compareTo(max) > 0) {
                    max = temp;
                }
            }
            if (!start.equals("")) {
                node.setHashStart(start);
            }
            else {
                node.setHashStart(max);
            }
            out.add(node);
        }
        return out;
    }

    public static void main(String[] args) {
        // TODO
    }
}
