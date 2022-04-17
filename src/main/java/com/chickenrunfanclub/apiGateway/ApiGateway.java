package com.chickenrunfanclub.apiGateway;

import com.chickenrunfanclub.app_kvECS.AllServerMetadata;
import com.chickenrunfanclub.app_kvServer.KVServer;
import com.chickenrunfanclub.shared.IRunning;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class ApiGateway extends Thread implements IApiGateway, IRunning {
    private int port;
    private AllServerMetadata allServerMetadata;
    private List<ApiGatewayClientConnection> threads;

    private static final Logger logger = LogManager.getLogger(ApiGateway.class);
    private ServerSocket serverSocket;
    private boolean running;

    public ApiGateway(int port) {
        this.port = port;
        allServerMetadata = new AllServerMetadata();
        threads = new ArrayList<>();
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub
        running = initializeServer();

        if (serverSocket != null) {
            while (isRunning()) {
                try {
                    Socket client = serverSocket.accept();
                    // TODO: REPLACE
                    ApiGatewayClientConnection connection = new ApiGatewayClientConnection(client, this);
                    threads.add(connection);
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

    public boolean isRunning() {
        return running;
    }

    @Override
    public void close() {
        running = false;

        long start = System.currentTimeMillis();
        long end = start + 5 * 1000L;
        while (System.currentTimeMillis() < end) {}

        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.error("Error! " + "Unable to close ApiGateway", e);
        }
    }

    public void replaceAllServerMetadata(AllServerMetadata replacer) {
        // TODO: Since we are passing this to the client connections, we really need to create an update method
        // TODO: on allServerMetadata so they actually get the updated ones
        allServerMetadata = replacer;
    }

    public AllServerMetadata getAllServerMetadata() {
        return allServerMetadata;
    }

    public String heartBeat() {
        return "Hello";
    }

    private boolean initializeServer() {
        logger.debug("Initialize Api Gateway ...");
        try {
            serverSocket = new ServerSocket(port);
            logger.debug("Api Gateway listening on port: "
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
