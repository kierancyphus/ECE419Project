package com.chickenrunfanclub.client;

import com.chickenrunfanclub.app_kvECS.AllServerMetadata;
import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.shared.Messenger;
import com.chickenrunfanclub.shared.messages.*;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Scanner;

public class KVStore implements KVCommInterface {
    private final String config_file;
    private ArrayList<String> allServers;
    private String address;
    private int port;
    private Messenger messenger;
    private Logger logger = LogManager.getLogger(KVStore.class);
    private AllServerMetadata meta;
    private boolean running;

    private Socket clientSocket;

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

    public KVStore(String address, int port, AllServerMetadata asm) {
        this.meta = asm;
        this.address = address;
        this.port = port;
        this.config_file = null;
    }

    /**
     * Initialize KVStore with address and port of KVServer
     *
     * @param address the address of the KVServer
     * @param port    the port of the KVServer
     */
    public KVStore(String address, int port) {
        this.meta = null;
        this.address = address;
        this.port = port;
        this.config_file = null;
    }

    public KVStore(String config_file) {
        this.meta = new AllServerMetadata(config_file);
        this.config_file = config_file;
        this.allServers = new ArrayList<String>();
        processConfig(config_file);
    }

    public void processConfig(String config_file) {
        File f = new File(config_file);
        try {
            Scanner scanner = new Scanner(f);
            while (scanner.hasNextLine()) {
                allServers.add(scanner.nextLine());
            }
        } catch (FileNotFoundException e) {
            System.out.println("Configuration file not found");
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized void closeConnection() {
        logger.info("try to close connection ...");

        try {
            tearDownConnection();
        } catch (IOException ioe) {
            logger.error("Unable to close connection!");
        }
    }

    private void tearDownConnection() throws IOException {
        setRunning(false);
        logger.info("tearing down the connection ...");
        if (clientSocket != null) {
            messenger.closeConnections();
            clientSocket = null;
            logger.info("connection closed!");
        }
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean run) {
        running = run;
    }

    @Override
    public void connect(String address, int port) throws IOException, UnknownHostException {
        clientSocket = new Socket(address, port);
        messenger = new Messenger(clientSocket);
        setRunning(true);
        logger.info("Connection established");
    }

    @Override
    public void disconnect() {
        if (clientSocket != null) {
            messenger.closeConnections();
            clientSocket = null;
        }
    }

    public IKVMessage sendAndReceiveMessage(String key, String value, IKVMessage.StatusType status) throws Exception {
        KVMessage message = new KVMessage(key, value, status);
        TextMessage textMessage = new TextMessage(message);
        messenger.sendMessage(textMessage);
        TextMessage response = messenger.receiveMessage();
        return new KVMessage(response);
    }

    public IKVMessage sendAndReceiveMessage(String key, String value, IKVMessage.StatusType status, int index) throws Exception {
        KVMessage message = new KVMessage(key, value, status, index);
        TextMessage textMessage = new TextMessage(message);
        messenger.sendMessage(textMessage);
        TextMessage response = messenger.receiveMessage();
        return new KVMessage(response);
    }


    public IServerMessage sendAndReceiveServerMessage(IServerMessage.StatusType status, String address, int port) throws Exception {
        connect(address, port);
        ServerMessage message = new ServerMessage(null, null, status);
        TextMessage textMessage = new TextMessage(message);
        messenger.sendMessage(textMessage);
        TextMessage response = messenger.receiveMessage();
        disconnect();
        return new ServerMessage(response);
    }

    public AllServerMetadata pollAll() {
        AllServerMetadata newMeta = null;
        for (String allServer : this.allServers) {
            try {
                String[] info = allServer.split(" ");
                connect(info[1], Integer.parseInt(info[2]));
                IKVMessage kvresponse = sendAndReceiveMessage("I LOVE ECE419", null, IKVMessage.StatusType.GET);
                IKVMessage.StatusType returnStatus = kvresponse.getStatus();
                if (returnStatus == IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
                    try {
                        newMeta = new Gson().fromJson(kvresponse.getKey(), AllServerMetadata.class);
                    } catch (JsonParseException e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
            }
        }
        if (newMeta == null) {
            logger.error("Critical servers are down so no requests can be processed right now. Please try again once shit is figured out");
        }
        return newMeta;
    }

    public IKVMessage moveDataPut(String key, String value, String host, int port) throws Exception {
        connect(host, port);
        IKVMessage response = sendAndReceiveMessage(key, value, IKVMessage.StatusType.MOVE_DATA_PUT);
        disconnect();
        return response;
    }

    public IKVMessage put(String key, String value, String host, int port, int index) throws IOException {
        int retries = 0;
        boolean connectionSuccess = false;
        IKVMessage response = null;

        while (retries < 3 && !connectionSuccess) {
            connect(host, port);
            try {
                response = sendAndReceiveMessage(key, value, IKVMessage.StatusType.PUT, index);
                connectionSuccess = true;
            } catch (Exception e) {
                response = new KVMessage(key, value, IKVMessage.StatusType.PUT_ERROR);
            }
            disconnect();

            retries++;
        }
        return response;
    }

    //    @Override
    public IKVMessage put(String key, String value, int index) {
        /*
        * This should only be used by the KVClient UI since it relies upon initialization from the config file
        * If you want to use this internally, use moveDataPut and initialize allServerMetadata first
        * */

        IKVMessage.StatusType returnStatus = IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE;
        IKVMessage kvresponse = null;
        int attempts = 0;

        while (returnStatus == IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE && attempts < 3) {
            try {
                ECSNode node_responsible = this.meta.findServerResponsible(key, false);
                connect(node_responsible.getHost(), node_responsible.getPort());

                kvresponse = sendAndReceiveMessage(key, value, IKVMessage.StatusType.PUT, index);
                returnStatus = kvresponse.getStatus();

                if (returnStatus == IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
                    try {
                        this.meta = new Gson().fromJson(kvresponse.getKey(), AllServerMetadata.class);
                    } catch (JsonParseException e) {
                        e.printStackTrace();
                    }
                }
                disconnect();

            } catch (IOException e) {
                logger.info(e);
                logger.info("About to poll all servers");
                this.meta = pollAll();
            } catch (Exception e) {
                e.printStackTrace();
            }
            attempts++;
        }
        return kvresponse;
    }

    @Override
    public IKVMessage get(String key) {
        IKVMessage.StatusType returnStatus = IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE;
        IKVMessage kvresponse = null;
        int attempts = 0;


        while (returnStatus == IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE && attempts < 3) {
            try {
                ECSNode node_responsible = this.meta.findServerResponsible(key, true);
                connect(node_responsible.getHost(), node_responsible.getPort());

                kvresponse = sendAndReceiveMessage(key, null, IKVMessage.StatusType.GET);
                returnStatus = kvresponse.getStatus();

                if (returnStatus == IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
                    try {
                        this.meta = new Gson().fromJson(kvresponse.getKey(), AllServerMetadata.class);
                    } catch (JsonParseException e) {
                        e.printStackTrace();
                    }
                }
                disconnect();
            } catch (IOException e) {
                this.meta = pollAll();
            } catch (Exception e) {
                e.printStackTrace();
            }

            attempts++;
        }
        return kvresponse;
    }

    @Override
    public IServerMessage start(String address, int port) {
        try {
            return sendAndReceiveServerMessage(IServerMessage.StatusType.SERVER_START, address, port);
        } catch (Exception e) {
            e.printStackTrace();
            return null;

        }
    }

    @Override
    public IServerMessage stop(String address, int port) {
        try {
            return sendAndReceiveServerMessage(IServerMessage.StatusType.SERVER_STOP, address, port);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public IServerMessage shutDown(String address, int port) {
        try {
            return sendAndReceiveServerMessage(IServerMessage.StatusType.SERVER_SHUTDOWN, address, port);
        } catch (Exception e) {
            logger.info(e);
            return null;

        }
    }

    @Override
    public IServerMessage lockWrite(String address, int port) {
        try {
            return sendAndReceiveServerMessage(IServerMessage.StatusType.SERVER_LOCK_WRITE, address, port);
        } catch (Exception e) {
            e.printStackTrace();
            return null;

        }
    }

    @Override
    public IServerMessage unlockWrite(String address, int port) {
        try {
            return sendAndReceiveServerMessage(IServerMessage.StatusType.SERVER_UNLOCK_WRITE, address, port);
        } catch (Exception e) {
            e.printStackTrace();
            return null;

        }

    }

    @Override
    public IServerMessage moveData(ECSNode metadata) throws Exception {
        String metadataString = new Gson().toJson(metadata, ECSNode.class);
        ServerMessage message = new ServerMessage(metadataString, null, IServerMessage.StatusType.SERVER_MOVE_DATA);
        TextMessage textMessage = new TextMessage(message);
        messenger.sendMessage(textMessage);
        TextMessage response = messenger.receiveMessage();
        return new ServerMessage(response);
    }

    @Override
    public IServerMessage updateMetadata(ECSNode metadata) throws Exception {
        String metadataString = new Gson().toJson(metadata, ECSNode.class);
        ServerMessage message = new ServerMessage(metadataString, null, IServerMessage.StatusType.SERVER_UPDATE_METADATA);
        TextMessage textMessage = new TextMessage(message);
        messenger.sendMessage(textMessage);
        TextMessage response = messenger.receiveMessage();
        return new ServerMessage(response);
    }

    public IServerMessage updateAllMetadata(AllServerMetadata asm) throws Exception {
        String asmString = new Gson().toJson(asm, AllServerMetadata.class);
        ServerMessage message = new ServerMessage(asmString, null, IServerMessage.StatusType.SERVER_UPDATE_ALL_METADATA);
        TextMessage textMessage = new TextMessage(message);
        messenger.sendMessage(textMessage);
        TextMessage response = messenger.receiveMessage();
        return new ServerMessage(response);
    }

    public IServerMessage sendHeartbeat() throws Exception {
        ServerMessage message = new ServerMessage("", null, IServerMessage.StatusType.SERVER_HEARTBEAT);
        TextMessage textMessage = new TextMessage(message);
        messenger.sendMessage(textMessage);
        TextMessage response = messenger.receiveMessage();
        return new ServerMessage(response);
    }
}
