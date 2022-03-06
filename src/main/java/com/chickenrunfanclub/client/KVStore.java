package com.chickenrunfanclub.client;

import com.chickenrunfanclub.shared.Messenger;
import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.shared.messages.IKVMessage;
import com.chickenrunfanclub.shared.messages.KVMessage;
import com.chickenrunfanclub.shared.messages.TextMessage;
import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class KVStore implements KVCommInterface {
    private String address;
    private int port;

    private Messenger messenger;
    private Logger logger = LogManager.getLogger(KVStore.class);

    private boolean running;

    private Socket clientSocket;

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

    /**
     * Initialize KVStore with address and port of KVServer
     *
     * @param address the address of the KVServer
     * @param port    the port of the KVServer
     */
    public KVStore(String address, int port) {
        this.address = address;
        this.port = port;
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
    public void connect() throws IOException, UnknownHostException {
        System.out.println(address + port);
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

    public IKVMessage sendAndReceiveMessage(IKVMessage.StatusType status) throws Exception {
        KVMessage message = new KVMessage(null, null, status);
        TextMessage textMessage = new TextMessage(message);
        messenger.sendMessage(textMessage);
        TextMessage response = messenger.receiveMessage();
        return new KVMessage(response);
    }

    @Override
    public IKVMessage put(String key, String value) throws Exception {
        KVMessage message = new KVMessage(key, value, IKVMessage.StatusType.PUT);
        TextMessage textMessage = new TextMessage(message);
        messenger.sendMessage(textMessage);
        TextMessage response = messenger.receiveMessage();
        return new KVMessage(response);
    }

    @Override
    public IKVMessage get(String key) throws Exception {
        KVMessage message = new KVMessage(key, null, IKVMessage.StatusType.GET);
        TextMessage textMessage = new TextMessage(message);
        messenger.sendMessage(textMessage);
        TextMessage response = messenger.receiveMessage();
        return new KVMessage(response);
    }

    @Override
    public IKVMessage start() throws Exception {
        return sendAndReceiveMessage(IKVMessage.StatusType.SERVER_START);
    }

    @Override
    public IKVMessage stop() throws Exception {
        return sendAndReceiveMessage(IKVMessage.StatusType.SERVER_STOP);
    }

    @Override
    public IKVMessage shutDown() throws Exception {
        return sendAndReceiveMessage(IKVMessage.StatusType.SERVER_STOP);
    }

    @Override
    public IKVMessage lockWrite() throws Exception {
        return sendAndReceiveMessage(IKVMessage.StatusType.SERVER_WRITE_LOCK);
    }

    @Override
    public IKVMessage unlockWrite() throws Exception {
        return sendAndReceiveMessage(IKVMessage.StatusType.SERVER_WRITE_UNLOCKED);
    }

    @Override
    public IKVMessage moveData(ECSNode metadata) throws Exception {
        String metadataString = new Gson().toJson(metadata, ECSNode.class);
        KVMessage message = new KVMessage(metadataString, null, IKVMessage.StatusType.SERVER_MOVE_DATA);
        TextMessage textMessage = new TextMessage(message);
        messenger.sendMessage(textMessage);
        TextMessage response = messenger.receiveMessage();
        return new KVMessage(response);
    }

    @Override
    public IKVMessage updateMetadata(ECSNode metadata) throws Exception {
        String metadataString = new Gson().toJson(metadata, ECSNode.class);
        KVMessage message = new KVMessage(metadataString, null, IKVMessage.StatusType.SERVER_UPDATE_METADATA);
        TextMessage textMessage = new TextMessage(message);
        messenger.sendMessage(textMessage);
        TextMessage response = messenger.receiveMessage();
        return new KVMessage(response);
    }
}
