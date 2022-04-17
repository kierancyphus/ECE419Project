package com.chickenrunfanclub.app_kvAPI;

import com.chickenrunfanclub.app_kvAuth.AuthClient;
import com.chickenrunfanclub.client.KVStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

public class KVAPIGateway {
    private static final Logger logger = LogManager.getLogger(KVAPIGateway.class);
    private KVStore internal;
    private int port;
    private int auth_port;
    private String auth_address;

    private ServerSocket serverSocket;
    private boolean running;

    public KVAPIGateway(int port, String auth_address, int auth_port) {
        this.port = port;
        this.auth_address = auth_address;
        this.auth_port = auth_port;
        //TODO: How to initialize the internal KVStore?
        this.internal = new KVStore(null);

    }

    // TODO: How to handle get and put internally?
    public String get(String key){return null;}

    public void put(String key, String value){}


    private boolean initializeServer() {
        logger.info("Initialize API ...");
        try {
            serverSocket = new ServerSocket(port);
            logger.info("API listening on port: "
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

    public boolean isRunning() {
        return running;
    }

    public void stop() {
        this.running = false;
    }

    public void run() {
        running = initializeServer();

        if (serverSocket != null) {
            while (isRunning()) {
                try {
                    Socket client = serverSocket.accept();
                    AuthClient auth = new AuthClient(auth_address, auth_port);
                    APIConnection connection = new APIConnection(client, auth, this);
                    new Thread(connection).start();
                    logger.info("API connected to "
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
}
