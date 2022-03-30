package com.chickenrunfanclub.app_kvServer;

import com.chickenrunfanclub.app_kvECS.AllServerMetadata;
import com.chickenrunfanclub.client.KVStore;
import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.shared.messages.IKVMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KVServer extends Thread implements IKVServer {
    private int port;
    private int cacheSize;
    private IKVServer.CacheStrategy strategy;
    private KVRepo repo;
    private ECSNode metadata;
    private AllServerMetadata allServerMetadata;

    private static final Logger logger = LogManager.getLogger(KVServer.class);
    private ServerSocket serverSocket;
    private boolean running;

    /**
     * Start KV Server at given port
     *
     * @param port      given port for storage server to operate
     * @param cacheSize specifies how many key-value pairs the server is allowed
     *                  to keep in-memory
     * @param strategy  specifies the cache replacement strategy in case the cache
     *                  is full and there is a GET- or PUT-request on a key that is
     *                  currently not contained in the cache. Options are "FIFO", "LRU",
     *                  and "LFU".
     */
    public KVServer(int port, int cacheSize, String strategy) {
        this.port = port;
        this.cacheSize = cacheSize;
        try {
            this.strategy = CacheStrategy.valueOf(strategy);
        } catch (IllegalArgumentException e) {
            logger.error("Error! Unknown cache strategy");
            return;
        }
        metadata = new ECSNode();
        this.repo = new KVRepo(cacheSize, this.strategy, metadata);
        allServerMetadata = null;

    }

    public KVServer(int port, int cacheSize, String strategy, String storePath) {
        this.port = port;
        this.cacheSize = cacheSize;
        try {
            this.strategy = CacheStrategy.valueOf(strategy);
        } catch (IllegalArgumentException e) {
            logger.error("Error! Unknown cache strategy");
            return;
        }
        metadata = new ECSNode();
        repo = new KVRepo(cacheSize, this.strategy, storePath, metadata);
        allServerMetadata = null;
    }


    @Override
    public int getPort() {
        // TODO Auto-generated method stub
        return this.port;
    }

    @Override
    public String getHostname() {
        return metadata.getHost();
    }

    @Override
    public CacheStrategy getCacheStrategy() {
        return this.strategy;
    }

    @Override
    public int getCacheSize() {
        return this.cacheSize;
    }

    @Override
    public boolean inStorage(String key) {
        return this.repo.inStorage(key);
    }

    @Override
    public boolean inCache(String key) {
        return this.repo.inCache(key);
    }

    @Override
    public String getKV(String key) throws Exception {
        IKVMessage response = this.repo.get(key);
        return response.getValue();
    }

    @Override
    public void putKV(String key, String value) throws Exception {
        this.repo.put(key, value);
    }

    @Override
    public void clearCache() {
        this.repo.clearCache();
    }

    @Override
    public void clearStorage() {
        clearCache();
        this.repo.nukeStore();
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub
        running = initializeServer();

        if (serverSocket != null) {
            while (isRunning()) {
                try {
                    Socket client = serverSocket.accept();
                    KVClientConnection connection = new KVClientConnection(client, repo, this);
                    new Thread(connection).start();
                    logger.info("Connected to "
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

    @Override
    public void kill() {
        // TODO Auto-generated method stub
        // TODO: keep track of threads and then figure out a safe way to kill them is probably best.
        // for now I'm just going to close the server
        close();
    }

    /**
     * Stops the server insofar that it won't listen at the given port anymore.
     */
    @Override
    public void close() {
        running = false;
        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.error("Error! " + "Unable to close socket on port: " + port, e);
        }
    }

    @Override
    public void lockWrite() {
        metadata.setWriteLock(true);
    }

    @Override
    public void unLockWrite() {
        metadata.setWriteLock(false);
    }

    @Override
    public boolean moveData(ECSNode ECSNode) {
        KVStore kvClient = new KVStore(ECSNode.getHost(), ECSNode.getPort());
        try {
            kvClient.connect(ECSNode.getHost(), ECSNode.getPort());
        } catch (IOException e) {
            logger.error("Could not connect to server: " + ECSNode.getHost() + ":" + ECSNode.getPort());
        }

        List<Map.Entry<String, String>> failed = new ArrayList<>();

        this.repo.getEntriesInHashRange(ECSNode)
                .forEach(entry -> {
                    try {
                        kvClient.put(entry.getKey(), entry.getValue());
                    } catch (Exception e) {
                        failed.add(entry);
                    }
                });

        return failed.size() == 0;
    }

    /**
     * Copies over field values from param: metadata to this.metadata. We can't directly assign it because then
     * KVRepo's copy of metadata doesn't update.
     */
    @Override
    public void updateMetadata(ECSNode ECSNode) {
        this.metadata.updateMetadata(ECSNode);
    }

    public void updateServerStopped(boolean stopped) {
        metadata.setServerLock(stopped);
    }

    public void replaceAllServerMetadata(AllServerMetadata replacer) {
        allServerMetadata = replacer;
    }

    public ECSNode getMetadata() { return metadata; }

    private boolean isRunning() {
        return running;
    }

    public Set<String> listKeys() {
        return repo.listKeys();
    }

    public AllServerMetadata getAllMetadata() {
        return allServerMetadata;
    }

    private boolean initializeServer() {
        logger.info("Initialize server ...");
        try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: "
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
}
