package com.chickenrunfanclub.app_kvECS;

import com.chickenrunfanclub.client.KVStore;
import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.ecs.ECSNode.ECSNodeFlag;
import com.chickenrunfanclub.ecs.IECSNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class ECSClient implements IECSClient {
    private static Logger logger = LogManager.getLogger(ECSClient.class);
    private ZooKeeper zk;
    private ArrayList<ECSNode> nodeList = new ArrayList<ECSNode>();
    private HashMap<String, String> metaData = new HashMap<String, String>();   // node to hash in use
    private HashMap<String, String> hashToName = new HashMap<String, String>();     // hash to node
    private HashMap<String, IECSNode> nameToNode = new HashMap<String, IECSNode>();     // hash to node
    private HashMap<String, ECSNodeFlag> serverNameToStatus = new HashMap<String, ECSNodeFlag>();     // node status
    private CountDownLatch connectedSignal;
    // this command is not working for some reason
    private static final String SCRIPT_TEXT = "ssh -n %s nohup \"java -jar ~/ece419/testing/M1/build/libs/ece419-1.3-SNAPSHOT-all.jar server %s %s %s\"";
//    private static final String SCRIPT_TEXT = "ssh -n %s nohup java -jar ./m2-server.jar %s %s %s &";
    private final int TIMEOUT = 15000;
    private int numServers = 0;
    private String cacheStrategy;
    private int cacheSize;
    private boolean running = false;
    private AllServerMetadata allServerMetadata;


    public ECSClient(String configFileName, String cacheStrat, int cacheSiz) throws IOException, InterruptedException, KeeperException {
        // start zookeeper connection
        connectedSignal = new CountDownLatch(1);
        cacheStrategy = cacheStrat;
        cacheSize = cacheSiz;
//        zk = new ZooKeeper("127.0.0.1", 5000, new Watcher() {
//            public void process(WatchedEvent we) {
//                if (we.getState() == KeeperState.SyncConnected) {
//                    connectedSignal.countDown();
//                }
//            }
//        });
//
//        zk.create("/ecs", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        zk.create("/ecs/metadata", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        // load all ECS nodes from config file
//        readFile(configFileName);
        allServerMetadata = new AllServerMetadata(configFileName);
        running = true;
    }

    @Override
    public boolean start() throws Exception {
        // this only starts the servers that are idle. Need to add servers before to have active ones when the service is running

        List<ECSNode> idleNodes = allServerMetadata.getAllNodesByStatus(ECSNodeFlag.IDLE);
        for (ECSNode node : idleNodes) {
            // start each of the servers
            KVStore client = new KVStore(node.getHost(), node.getPort());
            client.connect();
            client.start();
        }

        allServerMetadata.updateStatus(ECSNodeFlag.IDLE, ECSNodeFlag.START);
        return true;
//        running = true;
//        return running;
//        try {
//            List<String> nodes = zk.getChildren("/ecs", false);
//            for (String node : nodes) {
//                if (!node.equals("metadata")) {
//                    serverNameToStatus.put(node, ECSNodeFlag.IN_USE);
//                    metaData.put(node, Hasher.hash(node));
//                }
//            }
//            running = true;
//            return true;
//        } catch (Exception e) {
//            logger.error("Unable to start ECS. " + e);
//        }
//        return false;
    }

    @Override
    public boolean stop() throws Exception {
        List<ECSNode> idleNodes = allServerMetadata.getAllNodesByStatus(ECSNodeFlag.START);
        for (ECSNode node : idleNodes) {
            // start each of the servers
            KVStore client = new KVStore(node.getHost(), node.getPort());
            client.connect();
            client.stop();
        }

        allServerMetadata.updateStatus(ECSNodeFlag.START, ECSNodeFlag.STOP);
        return true;
//
//
//        List<String> nodes = zk.getChildren("/ecs", false);
//        for (String node : nodes) {
//            if (!node.equals("metadata")) {
//                serverNameToStatus.put(node, ECSNodeFlag.STOP);
//            }
//        }
//        running = false;
//        return true;
    }

    @Override
    public boolean shutdown() throws Exception {
        List<ECSNode> idleNodes = allServerMetadata.getAllNodesByStatus(ECSNodeFlag.START);
        for (ECSNode node : idleNodes) {
            // start each of the servers
            KVStore client = new KVStore(node.getHost(), node.getPort());
            client.connect();
            client.shutDown();
        }

        allServerMetadata.updateStatus(ECSNodeFlag.START, ECSNodeFlag.SHUT_DOWN);
        return true;

//        List<String> nodes = zk.getChildren("/ecs", false);
//        for (String node : nodes) {
//            if (!node.equals("metadata")) {
//                serverNameToStatus.put(node, ECSNodeFlag.SHUT_DOWN);
//                zk.delete("/ecs/" + node, -1);
//            }
//        }
//        zk.setData("/ecs/metadata", null, -1);
//        zk.delete("/ecs/metadata", -1);
//        zk.delete("/ecs", -1);
//        zk.close();
//        return true;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) throws InterruptedException, KeeperException {
        // sets the first stopped node to IDLE so that it can be started by start
        numServers++;
        ECSNode serverToAdd = allServerMetadata.getFirstByStatus(ECSNodeFlag.STOP);
        allServerMetadata.updateNodeStatus(serverToAdd, ECSNodeFlag.IDLE);

        Runtime run = Runtime.getRuntime();
        String script = String.format(SCRIPT_TEXT, serverToAdd.getHost(), serverToAdd.getPort(), serverToAdd.getCacheSize(), serverToAdd.getCacheStrategy());
        logger.info("About to run: " + script);
        try {
            Process proc = run.exec(script);
            int terminted = proc.waitFor();
            logger.info("starting new server terminated with signal: " + terminted);
        } catch (Exception e) {
            logger.error("can not add nodes " + e);
        }

        return serverToAdd;

//
//        try {
//            // check for available node first
//            if (metaData.size() >= nodeList.size()) {
//                logger.error("add node unsuccessful due to insufficient nodes.");
//                return null;
//            }
//            ECSNode node = null;
//            Set<String> nodesInUse = metaData.keySet();
//            // get the list of nodes we are setting up, update metadata and status
//            for (ECSNode n : nodeList) {
//                if (!nodesInUse.contains(n.getName())
//                        && (serverNameToStatus.get(n.getName()) == ECSNodeFlag.START
//                        || serverNameToStatus.get(n.getName()) == ECSNodeFlag.IDLE)) {
//                    node = n;
//                    node.setCacheSize(cacheSize);
//                    node.setCacheStrategy(cacheStrategy);
//                    node.setHashStart(null);
//                    nameToNode.put(node.getName(), node);
//                    metaData.put(node.getName(), node.getServerHashVal());
//                    numServers++;
//                    serverNameToStatus.put(node.getName(), ECSNodeFlag.IN_USE);
//                    // create zookeeper nodes
//                    zk.create("/ecs/" + node.getName(), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//                    break;
//                }
//            }
//            // writeMetaData();
//
//            Runtime run = Runtime.getRuntime();
//            String script = String.format(SCRIPT_TEXT, node.getHost(), node.getPort(), node.getCacheSize(), node.getCacheStrategy());
//            Process proc = run.exec(script);
//            proc.waitFor();
//        } catch (Exception e) {
//            logger.error("can not add nodes " + e);
//        }
//        return null;
    }

    @Override
    public List<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) throws InterruptedException, KeeperException {
        List<IECSNode> nodeList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            nodeList.add(addNode(cacheStrategy, cacheSize));
        }
        return nodeList;
//
//        try {
//            ArrayList<ECSNode> newNodes = setupNodes(count, cacheStrategy, cacheSize);
//            Runtime run = Runtime.getRuntime();
//            for (ECSNode node : newNodes) {
//                String script = String.format(SCRIPT_TEXT, node.getHost(), node.getPort(),
//                        node.getCacheSize(), node.getCacheStrategy());
//                Process proc = run.exec(script);
//                proc.waitFor();
//            }
//            if (awaitNodes(newNodes.size(), TIMEOUT)) {
//                return newNodes;
//            }
//        } catch (Exception e) {
//            logger.error("can not add nodes " + e);
//        }
//        return null;
    }

    @Override
    public ArrayList<ECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
//        // check for available node first
//        if (metaData.size() + count > nodeList.size()) {
//            logger.error("set up nodes unsuccessful due to insufficient nodes.");
//            return null;
//        }
//        ArrayList<ECSNode> nodes = new ArrayList<>();
//        Set<String> nodesInUse = metaData.keySet();
//        int counter = 0;
//        try {
//            // get the list of nodes we are setting up, update metadata and status
//            for (ECSNode node : nodeList) {
//                if (!nodesInUse.contains(node.getName())
//                        && (serverNameToStatus.get(node.getName()) == ECSNodeFlag.START
//                        || serverNameToStatus.get(node.getName()) == ECSNodeFlag.IDLE)) {
//                    node.setCacheSize(cacheSize);
//                    node.setCacheStrategy(cacheStrategy);
//                    node.setHashStart(null);
//                    nodes.add(node);
//                    nameToNode.put(node.getName(), node);
//                    // metaData.put(node.getName(), node.getServerHashVal());
//                    numServers++;
//                    serverNameToStatus.put(node.getName(), ECSNodeFlag.STOP);
//                    // create zookeeper nodes
//                    zk.create("/ecs/" + node.getName(), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//
//                    counter++;
//                    if (counter == count) {
//                        break;
//                    }
//                }
//            }
//            // writeMetaData();
//            return getNodeStartHashVal(nodes);
//        } catch (Exception e) {
//            logger.error("Set up nodes " + e);
//        }
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
                serverNameToStatus.put(node, ECSNodeFlag.SHUT_DOWN);
                metaData.remove(node);
                zk.setData("/ecs/" + node, null, -1);
                zk.delete("/ecs/" + node, -1);
            }
            writeMetaData();
            return true;
        } catch (KeeperException | InterruptedException | NoSuchAlgorithmException e) {
            logger.error(e);
            return false;
        }
    }

    public boolean removeNode(int nodeIdx) throws Exception {
        try {
            String node = nodeList.get(nodeIdx).getName();
            serverNameToStatus.put(node, ECSNodeFlag.SHUT_DOWN);
            metaData.remove(node);
            zk.setData("/ecs/" + node, null, -1);
            zk.delete("/ecs/" + node, -1);
            writeMetaData();
            return true;
        } catch (KeeperException | InterruptedException | NoSuchAlgorithmException e) {
            logger.error(e);
            return false;
        }
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        HashMap<String, IECSNode> map = new HashMap<String, IECSNode>();
//        for (String key : nameToNode.keySet()) {
//            if (serverStatus.get(key) == ECSNodeFlag.IN_USE) {
//                map.put(key, nameToNode.get(key));
//            }
//        }
//        // now need to organize the hash ranges for these nodes
//        String start, max, end, temp;
//        IECSNode node;
//        for (String key : map.keySet()) {
//            node = map.get(key);
//            start = "";
//            max = "";
//            end = node.getNodeHashRange()[1];
//            for (IECSNode n : map.values()){
//                temp = n.getNodeHashRange()[1];
//                if (temp.compareTo(end) < 0 && temp.compareTo(start) > 0){
//                    start = temp;
//                }
//                else if (temp.compareTo(max) > 0) {
//                    max = temp;
//                }
//            }
//            if (!start.equals("")) {
//                node.setHashStart(start);
//            }
//            else {
//                node.setHashStart(max);
//            }
//            map.put(key, node);
//        }
        return map;
    }

    @Override
    public IECSNode getNodeByKey(String key) {
        return allServerMetadata.findServerResponsible(key);
    }

    public boolean isRunning() {
        return running;
    }

//    private void readFile(String file) {
//        File f = new File(file);
//        String name, host, key, port, hashVal;
//        ECSNode node;
//        try {
//            Scanner scanner = new Scanner(f);
//            while (scanner.hasNextLine()) {
//                String[] line = scanner.nextLine().split(" ");
//                name = line[0];
//                host = line[1];
//                port = line[2];
//                key = host + ":" + port;
//                hashVal = Hasher.hash(key);
//                node = new ECSNode(name, host, Integer.parseInt(port), hashVal);
//                nodeList.add(node);
//                hashToName.put(hashVal, name);
//            }
//        } catch (FileNotFoundException e) {
//            System.out.println("Configuration file not found");
//            e.printStackTrace();
//            System.exit(1);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    private byte[] HashMapToByte(HashMap<String, String> map) throws IOException {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(byteOut);
        out.writeObject(map);
        return byteOut.toByteArray();
    }

    private void writeMetaData() throws Exception {
        zk.setData("/servers/metadata", HashMapToByte(metaData), -1);
    }
}
