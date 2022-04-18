package com.chickenrunfanclub.app_kvECS;

import com.chickenrunfanclub.client.KVExternalStore;
import com.chickenrunfanclub.client.KVInternalStore;
import com.chickenrunfanclub.client.KVStore;
import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.ecs.ECSNode.ECSNodeFlag;
import com.chickenrunfanclub.ecs.IECSNode;
import com.chickenrunfanclub.shared.messages.IKVMessage;
import com.chickenrunfanclub.shared.messages.IServerMessage;
import com.chickenrunfanclub.shared.messages.KVMessage;
import com.chickenrunfanclub.shared.messages.ServerMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import java.io.*;
import java.net.*;
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
    private static final String SCRIPT_GATEWAY_TEXT = "java -jar build/libs/ece419-1.3-SNAPSHOT-all.jar api %s &";
    private static final String SCRIPT_AUTH_TEXT = "java -jar build/libs/ece419-1.3-SNAPSHOT-all.jar auth %s &";

    private final int TIMEOUT = 15000;
    private int numServers = 0;
    private String cacheStrategy;
    private int cacheSize;
    private boolean running = false;
    private AllServerMetadata allServerMetadata;
    private ServerSocket serverSocket;
    private int port;
    private Heartbeat heartbeat;

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
        } catch (KeeperException.NodeExistsException e) {
            // logger.error(e);
        }

        allServerMetadata = new AllServerMetadata(configFileName);


        try {
            removeAllNodes();
        } catch (Exception e) {
            logger.info("Was unable to purge zookeeper :(");
            logger.debug(e);
        }


        numServers = zk.getChildren("/ecs", false).size();
        running = false;
        heartbeat = new Heartbeat(allServerMetadata, this);
        Thread t = new Thread(heartbeat);
        t.start();

        // need to start api gateway (always port 50500)
        startGateway(50500, allServerMetadata);
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

            // add them to the hash ring
            allServerMetadata.addNodeToHashRing(node);
        }

        allServerMetadata.updateStatus(ECSNodeFlag.IDLE, ECSNodeFlag.START);

        // broadcast metadata to all started nodes
        allServerMetadata.print();
        allServerMetadata.broadcastMetadata();
        return true;
    }

    @Override
    public boolean stop() throws Exception {
        List<ECSNode> idleNodes = allServerMetadata.getAllNodesByStatus(ECSNodeFlag.START);
        for (ECSNode node : idleNodes) {
            // stop each of the servers
            KVStore client = new KVStore(node.getHost(), node.getPort());
            client.stop(node.getHost(), node.getPort());

            // remove them from hash ring
            allServerMetadata.removeNodeFromHashRing(node);
        }

        allServerMetadata.updateStatus(ECSNodeFlag.START, ECSNodeFlag.STOP);

        // broadcast metadata changes
        allServerMetadata.broadcastMetadata();
        return true;
    }

    @Override
    public boolean shutdown() throws Exception {
        List<ECSNode> idleNodes = allServerMetadata.getAllNodesByStatus(ECSNodeFlag.START);
        KVInternalStore store = new KVInternalStore(allServerMetadata);

        for (ECSNode node : idleNodes) {
            // shutdown each of the servers
            IServerMessage response = store.shutdown(node.getHost(), node.getPort());
            logger.info(response);
        }

        // shutdown api gateway
        store.shutdown("localhost", allServerMetadata.getGateway().getPort());

        allServerMetadata.updateStatus(ECSNodeFlag.START, ECSNodeFlag.SHUT_DOWN);
        removeAllNodes();
        heartbeat.stop();
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

            while (true) {
                try {
                    KVStore client = new KVStore(serverToAdd.getHost(), serverToAdd.getPort());
                    client.connect(serverToAdd.getHost(), serverToAdd.getPort());
                    client.disconnect();
                    // client.shutDown();
                    break;
                } catch (Exception e) {
                    // logger.error(e);
                    TimeUnit.SECONDS.sleep(1);
                }
            }

            allServerMetadata.updateNodeStatus(serverToAdd, ECSNodeFlag.IDLE);
            logger.info("starting new server " + serverToAdd.getName());
            numServers++;

        } catch (Exception e) {
            System.out.println("can not add nodes " + e);
            logger.error("can not add nodes " + e);
        }

        return serverToAdd;
    }

    public void startGateway(int port, AllServerMetadata asm) {
        try {
            logger.debug("Starting Api Gateway on port: " + port);

            Runtime run = Runtime.getRuntime();
            String file = createScriptGateway(port);
            run.exec("chmod u+x " + file);
            Process proc = run.exec(file);
            proc.waitFor();

            BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String s;
            while ((s = stdInput.readLine()) != null) {
                System.out.println(s);
            }
            KVExternalStore client = new KVExternalStore("./apiGateway.cfg");
            client.setUsername("dummy");
            client.setPassword("dum");
            while (true) {
                try {
                    IKVMessage response = client.get("jkgaweffaj");
                    logger.info("polling gateway");
                    logger.info(response);
                    TimeUnit.SECONDS.sleep(1);
                    if (response.getStatus() != IKVMessage.StatusType.FAILED){
                        break;
                    }
                } catch (Exception e) {
                    // logger.error(e);
                    TimeUnit.SECONDS.sleep(1);
                }
                finally {
                    client.disconnect();
                }
            }
            asm.setGateway(new ECSNode("localhost", port));

        } catch (Exception e) {
            logger.error("Could not start api gateway: " + e);
        }
    }

    public void startAuth(int port) {
        try {
            logger.debug("Starting Auth on port: " + port);

            Runtime run = Runtime.getRuntime();
            String file = createScriptAuth(port);
            run.exec("chmod u+x " + file);
            Process proc = run.exec(file);
            proc.waitFor();

        } catch (Exception e) {
            logger.error("Could not start auth: " + e);
        }
    }

    public IECSNode addNode(ECSNode serverToAdd) throws InterruptedException, KeeperException, IOException {
        // sets the first stopped node to IDLE so that it can be started by start

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

            while (true) {
                try {
                    KVStore client = new KVStore(serverToAdd.getHost(), serverToAdd.getPort());
                    client.connect(serverToAdd.getHost(), serverToAdd.getPort());
                    client.disconnect();
                    // client.shutDown();
                    break;
                } catch (Exception e) {
                    // logger.error(e);
                    TimeUnit.SECONDS.sleep(1);
                }
            }

            allServerMetadata.updateNodeStatus(serverToAdd, ECSNodeFlag.IDLE);
            logger.info("starting new server " + serverToAdd.getName());
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

            PrintWriter out = new PrintWriter(new FileOutputStream("script.sh", false));

            InetAddress ia = InetAddress.getLocalHost();
            String script = String.format(SCRIPT_TEXT, node.getPort(), node.getCacheSize(), node.getCacheStrategy());
            if (!(Objects.equals(node.getHost(), "127.0.0.1") || Objects.equals(node.getHost(), "localhost"))) {
                script = "ssh -n " + node.getHost() + " nohup " + script;
            }
            out.println(script);
            out.close();
            return configFile.getAbsolutePath();
        } catch (FileNotFoundException e) {
            logger.debug(e);
            System.out.println("Cannot create ssh script");
        }
        return null;
    }

    public String createScriptGateway(int port) throws IOException {
        // only can be run on localhost rn
        try {
            File configFile = new File("script_gateway.sh");

            if (!configFile.exists()) {
                configFile.createNewFile();
            }

            PrintWriter out = new PrintWriter(new FileOutputStream("script_gateway.sh", false));

            String script = String.format(SCRIPT_GATEWAY_TEXT, port);
            out.println(script);
            out.close();
            return configFile.getAbsolutePath();
        } catch (FileNotFoundException e) {
            logger.debug(e);
            System.out.println("Cannot create ssh script");
        }
        return null;
    }

    public String createScriptAuth(int port) throws IOException {
        // only can be run on localhost rn
        try {
            File configFile = new File("script_auth.sh");

            if (!configFile.exists()) {
                configFile.createNewFile();
            }

            PrintWriter out = new PrintWriter(new FileOutputStream("script_gauth.sh", false));

            String script = String.format(SCRIPT_AUTH_TEXT, port);
            out.println(script);
            out.close();
            return configFile.getAbsolutePath();
        } catch (FileNotFoundException e) {
            logger.debug(e);
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
            List<ECSNode> usedNodes = allServerMetadata.getAllNodesByStatus(ECSNodeFlag.START);
            for (ECSNode node : usedNodes) {
                if (nodeNames.contains(node.getName())) {
                    // stop the node
                    allServerMetadata.updateNodeStatus(node, ECSNodeFlag.STOP);
                    // delete from zookeeper
                    zk.setData("/ecs/" + node.getName(), null, -1);
                    zk.delete("/ecs/" + node.getName(), -1);
                    n--;
                    numServers--;
                    logger.info("removed server " + node.getName());

                    KVStore kvClient = new KVStore(node.getHost(), node.getPort());
                    IServerMessage response = kvClient.shutDown(node.getHost(), node.getPort());
                    logger.info(response);
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
            List<ECSNode> nodes = allServerMetadata.getAllNodes();
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
        // TODO: check where this is used
        return allServerMetadata.findServerResponsible(key, true);
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
        } catch (Exception e) {
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



    public AllServerMetadata getMetadata(){
        return this.allServerMetadata;
    }
//    private void writeMetaData() throws Exception {
//        zk.setData("/servers/metadata", HashMapToByte(metaData), -1);
//    }
    public class Heartbeat implements Runnable {
        private static final int TIMEOUT = 10;
        private AllServerMetadata metadata;
        private ECSClient ecs;
        private boolean running = true;

        public Heartbeat(AllServerMetadata metadata, ECSClient ecs) {
            this.ecs = ecs;
            this.metadata = metadata;
        }

        public void stop() {
            this.running = false;
        }

        @Override
        public void run() {
            while (this.running) {
                logger.info("Sent heartbeat");
                ArrayList<ECSNode> deadNodes = new ArrayList<>();
                List<ECSNode> runningNodes = metadata.getAllNodesByStatus(ECSNodeFlag.START);
                for (ECSNode node : runningNodes) {
                    try {
                        KVStore client = new KVStore(node.getHost(), node.getPort());
                        IServerMessage hbResponse = client.sendHeartbeat(node.getHost(), node.getPort());
                        logger.info(hbResponse.getKey());
                    } catch (ConnectException e) {
                        logger.info("Dead node encountered!");
                        deadNodes.add(node);
//                        e.printStackTrace();
                    } catch (Exception e){
                        e.printStackTrace();
                    }
                }
                for (ECSNode node : deadNodes) {
                    try {
                        System.out.println("restarting server at port " + node.getPort());
                        Runtime run = Runtime.getRuntime();
                        String file = createScript(node);
                        run.exec("chmod u+x " + file);
                        Process proc = run.exec(file);
                        proc.waitFor();

                        while (true) {
                            try {
                                KVStore client = new KVStore(node.getHost(), node.getPort());
                                client.connect(node.getHost(), node.getPort());
                                client.disconnect();
                                break;
                            } catch (Exception e) {
                                 logger.error(e);
                                stall(1);
                            }
                        }
                    } catch (Exception e){
                        e.printStackTrace();
                    }
                }
                stall(TIMEOUT);
            }
            logger.info("Heartbeat shutting down!");

        }

        public void stall(int seconds) {
            long start = System.currentTimeMillis();
            long end = start + seconds * 1000L;
            while (System.currentTimeMillis() < end) {
            }
        }
    }
}