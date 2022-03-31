package com.chickenrunfanclub.app_kvECS;

import com.chickenrunfanclub.client.KVStore;
import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.ecs.ECSNode.ECSNodeFlag;
import com.chickenrunfanclub.ecs.IECSNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import java.io.*;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ECSClient implements IECSClient {
    private static Logger logger = LogManager.getLogger(ECSClient.class);
    private ZooKeeper zk;
    private ArrayList<ECSNode> nodeList = new ArrayList<ECSNode>();
    private HashMap<String, String> metaData = new HashMap<String, String>();   // node to hash in use
    private HashMap<String, String> hashToName = new HashMap<String, String>();     // hash to node
    private HashMap<String, IECSNode> nameToNode = new HashMap<String, IECSNode>();     // hash to node
    private HashMap<String, ECSNodeFlag> serverNameToStatus = new HashMap<String, ECSNodeFlag>();     // node status
    private CountDownLatch connectedSignal;
  
    private static final String SCRIPT_TEXT = "java -jar build/libs/ece419-1.3-SNAPSHOT-all.jar server %s %s %s &";

    private final int TIMEOUT = 15000;
    private int numServers = 0;
    private String cacheStrategy;
    private int cacheSize;
    private boolean running = false;
    private AllServerMetadata allServerMetadata;
    private ServerSocket serverSocket;
    private int port;


    public ECSClient(String configFileName, String cacheStrat, int cacheSiz, int port) throws IOException, InterruptedException, KeeperException {
        // start zookeeper connection
        ProcessBuilder zookeeperProcessBuilder =
                new ProcessBuilder("apache-zookeeper-3.6.3-bin/bin/zkServer.sh", "start", "apache-zookeeper-3.6.3-bin/conf/zoo.cfg")
                        .inheritIO();
        Process zookeeperProcess = zookeeperProcessBuilder.inheritIO().start();
        zookeeperProcess.waitFor();

        connectedSignal = new CountDownLatch(1);
        cacheStrategy = cacheStrat;
        cacheSize = cacheSiz;
        this.port = port;
        zk = new ZooKeeper("localhost", 5000, new Watcher() {
            public void process(WatchedEvent we) {
                if (we.getState() == KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            }
        });
        connectedSignal.await();
        try {
//            if (zk.exists("/ecs", false) != null) {
//                zk.create("/ecs", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//            }
            zk.create("/ecs", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        catch (KeeperException.NodeExistsException e){
            // logger.error(e);
        }

        allServerMetadata = new AllServerMetadata(configFileName);
        numServers = zk.getChildren("/ecs", false).size();
        running = false;
    }

    @Override
    public boolean start() throws Exception {
        // this only starts the servers that are idle. Need to add servers before to have active ones when the service is running
        running = true;
        List<ECSNode> idleNodes = allServerMetadata.getAllNodesByStatus(ECSNodeFlag.IDLE);
        for (ECSNode node : idleNodes) {
            // start each of the servers
            KVStore client = new KVStore(node.getHost(), node.getPort());
            client.start(node.getHost(), node.getPort());
        }

        allServerMetadata.updateStatus(ECSNodeFlag.IDLE, ECSNodeFlag.START);
        return true;
    }

    @Override
    public boolean stop() throws Exception {
        List<ECSNode> idleNodes = allServerMetadata.getAllNodesByStatus(ECSNodeFlag.START);
        for (ECSNode node : idleNodes) {
            // stop each of the servers
            KVStore client = new KVStore(node.getHost(), node.getPort());
            client.stop(node.getHost(), node.getPort());
        }

        allServerMetadata.updateStatus(ECSNodeFlag.START, ECSNodeFlag.STOP);
        return true;
    }

    @Override
    public boolean shutdown() throws Exception {
        List<ECSNode> idleNodes = allServerMetadata.getAllNodesByStatus(ECSNodeFlag.START);
        for (ECSNode node : idleNodes) {
            // shutdown each of the servers
            KVStore client = new KVStore(node.getHost(), node.getPort());
            client.shutDown(node.getHost(), node.getPort());
        }

        allServerMetadata.updateStatus(ECSNodeFlag.START, ECSNodeFlag.SHUT_DOWN);
        removeAllNodes();
        return true;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) throws InterruptedException, KeeperException, IOException {
        // sets the first stopped node to IDLE so that it can be started by start
        ECSNode serverToAdd;
        try {
            serverToAdd = allServerMetadata.getFirstByStatus(ECSNodeFlag.STOP);
        } catch (NoSuchElementException e) {
            logger.info("Failed to add a node because no nodes available.");
            return null;
        }

        String path = "/ecs/" + serverToAdd.getName();
        zk.create(path, nodeToByte(serverToAdd), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        try {
            System.out.println("creating server at port " + serverToAdd.getPort());
            Runtime run = Runtime.getRuntime();
            String file = createScript(serverToAdd);
            run.exec("chmod u+x " + file);
            Process proc = run.exec(file);
            proc.waitFor();
            //TimeUnit.SECONDS.sleep(5);

            BufferedReader stdInput = new BufferedReader(new
                    InputStreamReader(proc.getInputStream()));
            String s = null;
            while ((s = stdInput.readLine()) != null) {
                System.out.println(s);
            }

            while (true){
                try{
                    KVStore client = new KVStore(serverToAdd.getHost(), serverToAdd.getPort());
                    client.connect(serverToAdd.getHost(), serverToAdd.getPort());
                    // client.shutDown();
                    break;
                } catch (Exception e) {
                    // logger.error(e);
                    TimeUnit.SECONDS.sleep(1);
                }
            }

            allServerMetadata.updateNodeStatus(serverToAdd, ECSNodeFlag.IDLE);
            logger.info("starting new server "+ serverToAdd.getName());
            numServers++;

        } catch (Exception e) {
            System.out.println("can not add nodes " + e);
            logger.error("can not add nodes " + e);
        }

        return serverToAdd;
    }

    public String createScript(ECSNode node) throws IOException {

        try {
            File configFile = new File("script.sh");

            if (!configFile.exists()) {
                configFile.createNewFile();
            }

            PrintWriter out = new PrintWriter(new FileOutputStream("script.sh",
                    false));

            InetAddress ia = InetAddress.getLocalHost();
            String hostname = ia.getHostName();

            String sPath = configFile.getAbsolutePath();
            String script = String.format(SCRIPT_TEXT, node.getPort(), node.getCacheSize(), node.getCacheStrategy());
            if (!(Objects.equals(node.getHost(), "127.0.0.1") || Objects.equals(node.getHost(), "localhost"))){
                script = "ssh -n " + node.getHost() + " nohup " + script;
            }
            //script = "ssh -n " + node.getHost() + " nohup " + script;
            out.println(script);

            out.close();
            return configFile.getAbsolutePath();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("Cannot create ssh script");
        }

        return null;

    }

    @Override
    public List<IECSNode> addNodes(int count) throws Exception {
        int numServer = allServerMetadata.getAllNodesByStatus(ECSNodeFlag.STOP).size();
        if (numServer < count) {
            logger.info("Failed to add nodes. There are only " + numServer + " servers available.");
            return null;
        }
        List<IECSNode> nodeList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            //System.out.println(i);
            nodeList.add(addNode(cacheStrategy, cacheSize));
        }
        awaitNodes(count, 10000);
        return nodeList;
    }

    @Override
    public ArrayList<ECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        long startTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - startTime < timeout) {
            List<String> list = zk.getChildren("/ecs", true);
            if (numServers == list.size()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) throws Exception {
        try {
            int n = nodeNames.size();
            List<ECSNode> usedNodes =  allServerMetadata.getAllNodesByStatus(ECSNodeFlag.START);
            for (ECSNode node : usedNodes) {
                if (nodeNames.contains(node.getName())) {
                    // stop the node
                    allServerMetadata.updateNodeStatus(node, ECSNodeFlag.STOP);
                    // delete from zookeeper
                    zk.setData("/ecs/" + node.getName(), null, -1);
                    zk.delete("/ecs/" + node.getName(), -1);
                    n--;
                    numServers--;
                    logger.info("removed server "+ node.getName());
                }
            }
            if (n == 0)
                return true;
            return false;
        } catch (KeeperException | InterruptedException e) {
            logger.error(e);
            return false;
        }
    }

    public boolean removeNode(int nodeIdx) throws Exception {
        try {
            ECSNode node = allServerMetadata.getAllNodesByStatus(ECSNodeFlag.START).get(nodeIdx);
            List<String> nodeName = Arrays.asList(node.getName());
            removeNodes(nodeName);
            numServers--;
            return true;
        } catch (KeeperException | InterruptedException | NoSuchAlgorithmException e) {
            logger.error(e);
            return false;
        } catch (IndexOutOfBoundsException e) {
            logger.info("The node index you entered is not running.");
            return false;
        }
    }

    public boolean removeAllNodes() throws Exception {
        try {
            List<ECSNode> nodes =  allServerMetadata.getAllNodes();
            for (ECSNode node : nodes) {
                if (zk.exists("/ecs/" + node.getName(), false) != null) {
                    // stop the node
                    allServerMetadata.updateNodeStatus(node, ECSNodeFlag.STOP);
                    // delete from zookeeper
                    zk.setData("/ecs/" + node.getName(), null, -1);
                    zk.delete("/ecs/" + node.getName(), -1);
                }
            }
            return true;
        } catch (KeeperException | InterruptedException e) {
            logger.error(e);
            return false;
        }
    }

    public boolean shutdownServers() throws Exception {
        List<ECSNode> nodes = allServerMetadata.getAllNodes();
        for (ECSNode node : nodes) {
            // shutdown each of the servers
            KVStore client = new KVStore(node.getHost(), node.getPort());
            client.shutDown(node.getHost(), node.getPort());
        }

        allServerMetadata.updateStatus(ECSNodeFlag.START, ECSNodeFlag.SHUT_DOWN);
        removeAllNodes();
        return true;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        List<ECSNode> nodes = new ArrayList<ECSNode>(allServerMetadata.getHashToServer().values());
        HashMap<String, IECSNode> map = new HashMap<String, IECSNode>();
        for (ECSNode node : nodes) {
            map.put(node.getName(), node);
        }
        return map;
    }

    @Override
    public IECSNode getNodeByKey(String key) {
        return allServerMetadata.findServerResponsible(key);
    }

    public boolean isRunning() {
        return running;
    }

    private byte[] nodeToByte(Object node) throws IOException {
        byte[] bytes = null;
        try {
            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(byteOut);
            out.writeObject(node);
            out.flush();
            bytes = byteOut.toByteArray();
        }
        catch (Exception e) {
            logger.error(e);
        }
        return bytes;

    }

    public int getNumServers() {
        return allServerMetadata.getAllNodesByStatus(ECSNodeFlag.START).size();
    }

    public int zookeeperNodes() throws InterruptedException, KeeperException {
        return zk.getChildren("/ecs", false).size();
    }

    public void run() {
        running = initializeServer();

        if (serverSocket != null) {
            while (isRunning()) {
                try {
                    Socket client = serverSocket.accept();
                    ECSClientConnection connection = new ECSClientConnection(client, this);
                    new Thread(connection).start();
                    logger.info("ECS connected to "
                            + client.getInetAddress().getHostName()
                            + " on port " + client.getPort());
                } catch (IOException e) {
                    logger.error("Error! " +
                            "Unable to establish connection. \n", e);
                }
            }
        }
        logger.info("Server stopped.");
    }

    private boolean initializeServer() {
        logger.info("Initialize ECS ...");
        try {
            serverSocket = new ServerSocket(port);
            logger.info("ECS listening on port: "
                    + serverSocket.getLocalPort());
            return true;

        } catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }

//    private void writeMetaData() throws Exception {
//        zk.setData("/servers/metadata", HashMapToByte(metaData), -1);
//    }

}