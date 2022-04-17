package com.chickenrunfanclub.client;

import com.chickenrunfanclub.app_kvECS.AllServerMetadata;
import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.shared.Messenger;
import com.chickenrunfanclub.shared.messages.IKVMessage;
import com.chickenrunfanclub.shared.messages.KVMessage;
import com.chickenrunfanclub.shared.messages.TextMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class KVExternalStore implements IKVExternalStore {
    private AllServerMetadata asm;
    private final static Logger logger = LogManager.getLogger(KVExternalStore.class);
    private Socket clientSocket;
    private Messenger messenger;
    private String username;
    private String password;

    public KVExternalStore(String configFile) {
        // for ease of implementation we'll keep using asm, with just a single entry being responsible for everything
        this.asm = new AllServerMetadata(configFile);
        this.asm.getAllNodes().forEach(asm::addNodeToHashRing);
    }


    @Override
    public IKVMessage get(String key) {
        if ((username == null) || (password == null)) {
            return new KVMessage(key, null, IKVMessage.StatusType.NO_CREDENTIALS);
        }

        IKVMessage kvresponse = null;
        int attempts = 0;

        while (attempts < 3) {
            try {
                ECSNode nodeResponsible = asm.findServerResponsible(key, true);

                connect(nodeResponsible.getHost(), nodeResponsible.getPort());
                kvresponse = sendAndReceiveMessage(key, null, IKVMessage.StatusType.GET, 0, username, password);
                disconnect();
            } catch (Exception e) {
                logger.debug(e);
            }

            attempts++;
        }
        return kvresponse;

    }

    @Override
    public IKVMessage put(String key, String value, int index) {
        if ((username == null) || (password == null)) {
            return new KVMessage(key, value, IKVMessage.StatusType.NO_CREDENTIALS);
        }

        IKVMessage kvresponse = null;
        int attempts = 0;

        while (attempts < 3) {
            try {
                ECSNode node_responsible = asm.findServerResponsible(key, false);
                connect(node_responsible.getHost(), node_responsible.getPort());

                kvresponse = sendAndReceiveMessage(key, value, IKVMessage.StatusType.PUT, index, username, password);
                disconnect();

            } catch (Exception e) {
                logger.debug(e);
            }
            attempts++;
        }

        return kvresponse;
    }

    public void connect(String address, int port) throws IOException, UnknownHostException {
        clientSocket = new Socket(address, port);
        messenger = new Messenger(clientSocket);
        logger.info("Connection established");
    }

    public void disconnect() {
        if (clientSocket != null) {
            messenger.closeConnections();
            clientSocket = null;
        }
    }

    public IKVMessage sendAndReceiveMessage(String key, String value, IKVMessage.StatusType status, int index, String username, String password) throws Exception {
        KVMessage message = new KVMessage(key, value, status, index, username, password);
        TextMessage textMessage = new TextMessage(message);
        messenger.sendMessage(textMessage);
        TextMessage response = messenger.receiveMessage();
        return new KVMessage(response);
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public AllServerMetadata getAsm() {
        return asm;
    }

    public void setAsm(AllServerMetadata asm) {
        this.asm = asm;
    }
}
