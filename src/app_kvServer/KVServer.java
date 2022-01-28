package app_kvServer;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.messages.IKVMessage;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Objects;

public class KVServer extends Thread implements IKVServer {
    private String hostname;
    private int port;
    int cacheSize;
    IKVServer.CacheStrategy strategy;
    KVRepo repo;

    private static Logger logger = Logger.getRootLogger();
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
        this.repo = new KVRepo();
        if (Objects.equals(strategy, "FIFO")) {
            this.strategy = CacheStrategy.FIFO;
        } else if (Objects.equals(strategy, "LRU")) {
            this.strategy = CacheStrategy.LRU;
        } else {
            this.strategy = CacheStrategy.LFU;
        }
    }

    @Override
    public int getPort() {
        // TODO Auto-generated method stub
        return this.port;
    }

    @Override
    public String getHostname() {
        // TODO Auto-generated method stub
        return null;
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
        // TODO Auto-generated method stub
        return false;
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
        // TODO Auto-generated method stub
    }

    @Override
    public void clearStorage() {
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
                    KVClientConnection connection = new KVClientConnection(client, repo);
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

    private boolean isRunning() {
        return this.running;
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

    /**
     * Main entry point for the m1 server application.
     *
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
            new LogSetup("logs/server.log", Level.ALL);
            if (args.length != 3) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: Server <port> <cacheSize> <stategy>!");
            } else {
                int port = Integer.parseInt(args[0]);
                int cacheSize = Integer.parseInt(args[1]);
                String strategy = args[2];
                new KVServer(port, cacheSize, strategy).start();
            }
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException nfe) {
            System.out.println("Error! Invalid argument <port>! Not a number!");
            System.out.println("Usage: Server <port>!");
            System.exit(1);
        }
    }

}
