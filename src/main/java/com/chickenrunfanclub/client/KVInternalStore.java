package com.chickenrunfanclub.client;

import com.chickenrunfanclub.app_kvECS.AllServerMetadata;
import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.shared.Messenger;
import com.chickenrunfanclub.shared.messages.*;
import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class KVInternalStore implements IKVInternalStore{
    private AllServerMetadata asm;
    private final static Logger logger = LogManager.getLogger(KVExternalStore.class);
    private Socket clientSocket;
    private Messenger messenger;

    public KVInternalStore(AllServerMetadata asm) {
        this.asm = asm;
    }

    @Override
    public IServerMessage start(String host, int port) {
        IServerMessage response = new ServerMessage(null, null, IServerMessage.StatusType.FAILED);
        try {
            connect(host, port);
            response = sendAndReceiveServerMessage(IServerMessage.StatusType.SERVER_START);
        } catch (Exception e) {
            logger.debug(e);
        } finally {
            disconnect();
        }

        return response;
    }

    @Override
    public IServerMessage stop(String host, int port) {
        IServerMessage response = new ServerMessage(null, null, IServerMessage.StatusType.FAILED);
        try {
            connect(host, port);
            response = sendAndReceiveServerMessage(IServerMessage.StatusType.SERVER_STOP);
        } catch (Exception e) {
            logger.debug(e);
        } finally {
            disconnect();
        }

        return response;
    }

    @Override
    public IServerMessage shutdown(String host, int port) {
        IServerMessage response = new ServerMessage(null, null, IServerMessage.StatusType.FAILED);
        try {
            connect(host, port);
            response = sendAndReceiveServerMessage(IServerMessage.StatusType.SERVER_SHUTDOWN);
        } catch (Exception e) {
            logger.debug(e);
        } finally {
            disconnect();
        }

        return response;
    }

    @Override
    public IServerMessage lockWrite(String host, int port) {
        IServerMessage response = new ServerMessage(null, null, IServerMessage.StatusType.FAILED);
        try {
            connect(host, port);
            response = sendAndReceiveServerMessage(IServerMessage.StatusType.SERVER_LOCK_WRITE);
        } catch (Exception e) {
            logger.debug(e);
        } finally {
            disconnect();
        }

        return response;
    }

    @Override
    public IServerMessage unlockWrite(String host, int port) {
        IServerMessage response = new ServerMessage(null, null, IServerMessage.StatusType.FAILED);
        try {
            connect(host, port);
            response = sendAndReceiveServerMessage(IServerMessage.StatusType.SERVER_UNLOCK_WRITE);
        } catch (Exception e) {
            logger.debug(e);
        } finally {
            disconnect();
        }

        return response;
    }

    @Override
    public IServerMessage moveData(ECSNode metadata, String host, int port) {
        IServerMessage response = new ServerMessage(null, null, IServerMessage.StatusType.FAILED);
        try {
            connect(host, port);
            String metadataString = new Gson().toJson(metadata, ECSNode.class);
            ServerMessage message = new ServerMessage(metadataString, null, IServerMessage.StatusType.SERVER_MOVE_DATA);
            TextMessage textMessage = new TextMessage(message);
            messenger.sendMessage(textMessage);
            TextMessage text = messenger.receiveMessage();
            response = new ServerMessage(text);
        } catch (Exception e) {
            logger.debug(e);
        } finally {
            disconnect();
        }

        return response;
    }

    @Override
    public IServerMessage updateMetadata(ECSNode metadata, String host, int port) {
        IServerMessage response = new ServerMessage(null, null, IServerMessage.StatusType.FAILED);
        try {
            connect(host, port);
            String metadataString = new Gson().toJson(metadata, ECSNode.class);
            ServerMessage message = new ServerMessage(metadataString, null, IServerMessage.StatusType.SERVER_UPDATE_METADATA);
            TextMessage textMessage = new TextMessage(message);
            messenger.sendMessage(textMessage);
            TextMessage text = messenger.receiveMessage();
            response = new ServerMessage(text);
        } catch (Exception e) {
            logger.debug(e);
        } finally {
            disconnect();
        }

        return response;
    }

    @Override
    public IServerMessage updateAllMetadata(AllServerMetadata asm, String host, int port) {
        IServerMessage response = new ServerMessage(null, null, IServerMessage.StatusType.FAILED);
        try {
            connect(host, port);
            String metadataString = new Gson().toJson(asm, ECSNode.class);
            ServerMessage message = new ServerMessage(metadataString, null, IServerMessage.StatusType.SERVER_UPDATE_ALL_METADATA);
            TextMessage textMessage = new TextMessage(message);
            messenger.sendMessage(textMessage);
            TextMessage text = messenger.receiveMessage();
            response = new ServerMessage(text);
        } catch (Exception e) {
            logger.debug(e);
        } finally {
            disconnect();
        }

        return response;
    }

    @Override
    public IServerMessage sendHeartbeat(String host, int port) {
        IServerMessage response = new ServerMessage(null, null, IServerMessage.StatusType.FAILED);
        try {
            connect(host, port);
            response = sendAndReceiveServerMessage(IServerMessage.StatusType.SERVER_HEARTBEAT);
        } catch (Exception e) {
            logger.debug(e);
        } finally {
            disconnect();
        }

        return response;
    }

    @Override
    public IKVMessage get(String key) {
        IKVMessage kvresponse = new KVMessage(key, null, IKVMessage.StatusType.GET_ERROR);
        int attempts = 0;

        while (attempts < 3) {
            try {
                ECSNode nodeResponsible = asm.findServerResponsible(key, true);
                connect(nodeResponsible.getHost(), nodeResponsible.getPort());
                kvresponse = sendAndReceiveMessage(key, null, IKVMessage.StatusType.GET, 0);
            } catch (Exception e) {
                logger.debug(e);
            } finally {
                disconnect();
            }

            attempts++;
        }
        return kvresponse;

    }

    @Override
    public IKVMessage put(String key, String value, int index) {
        IKVMessage kvresponse = new KVMessage(key, null, IKVMessage.StatusType.GET_ERROR);
        int attempts = 0;

        while (attempts < 3) {
            try {
                ECSNode node_responsible = asm.findServerResponsible(key, false);
                connect(node_responsible.getHost(), node_responsible.getPort());

                kvresponse = sendAndReceiveMessage(key, value, IKVMessage.StatusType.PUT, index);

            } catch (Exception e) {
                logger.debug(e);
            } finally {
                disconnect();
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

    public IKVMessage sendAndReceiveMessage(String key, String value, IKVMessage.StatusType status, int index) throws Exception {
        // this differs from the external kvstore, where we append the username and password to everything.
        // Since this is an internal client, we assume that everything that uses this is already authed.
        KVMessage message = new KVMessage(key, value, status, index);
        TextMessage textMessage = new TextMessage(message);
        messenger.sendMessage(textMessage);
        TextMessage response = messenger.receiveMessage();
        return new KVMessage(response);
    }

    public IServerMessage sendAndReceiveServerMessage(IServerMessage.StatusType status) throws Exception {
        ServerMessage message = new ServerMessage(null, null, status);
        TextMessage textMessage = new TextMessage(message);
        messenger.sendMessage(textMessage);
        TextMessage response = messenger.receiveMessage();
        return new ServerMessage(response);
    }
}
