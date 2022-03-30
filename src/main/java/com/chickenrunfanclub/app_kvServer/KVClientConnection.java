package com.chickenrunfanclub.app_kvServer;

import com.chickenrunfanclub.app_kvECS.AllServerMetadata;
import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.shared.messages.*;
import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.chickenrunfanclub.shared.Messenger;

import java.io.IOException;
import java.net.Socket;

/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the client.
 */
public class KVClientConnection implements Runnable {

    private static final Logger logger = LogManager.getLogger(KVClientConnection.class);
    private final Messenger messenger;
    private final KVRepo repo;
    private final KVServer server;
    private boolean isOpen;

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     *
     * @param clientSocket the Socket object for the client connection.
     */
    public KVClientConnection(Socket clientSocket, KVRepo repo, KVServer server) {
        logger.info("Initializing the Client connection with " + clientSocket.getPort());
        this.isOpen = true;
        this.messenger = new Messenger(clientSocket);
        this.repo = repo;
        this.server = server;
    }

    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try {

            IKVMessage message = null;
            IKVMessage response = null;
            IServerMessage servermessage = null;
            IServerMessage serverresponse = null;
            boolean KV;

            while (isOpen) {
                try {
                    // sometimes the client sends weird empty messages, so this loop ensures we ignore those
                    TextMessage latestMsg = null;
                    while (latestMsg == null || latestMsg.getMsg().trim().length() < 1) {
                        latestMsg = messenger.receiveMessage();
                    }

                    try {
                        message = new KVMessage(latestMsg);
                        KV = true;
                    } catch (Exception e) {
                        servermessage = new ServerMessage(latestMsg);
                        KV = false;
                    }
                    response = null;
                    serverresponse = null;
                    if (KV) {
                        switch (message.getStatus()) {
                            case GET: {
                                if (server.getMetadata().notResponsibleFor(message.getKey())) {
                                    // handle here
                                    String allServerMetadata = new Gson().toJson(server.getAllMetadata(), AllServerMetadata.class);
                                    response = new KVMessage(allServerMetadata, null, IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
                                } else {
                                    response = repo.get(message.getKey());
                                }
                                break;
                            }
                            case PUT: {
                                if (server.getMetadata().notResponsibleFor(message.getKey())) {
                                    // handle here
                                    String allServerMetadata = new Gson().toJson(server.getAllMetadata(), AllServerMetadata.class);
                                    response = new KVMessage(allServerMetadata, null, IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
                                } else {
                                    response = repo.put(message.getKey(), message.getValue());
                                }
                                break;

                            }
                            case MOVE_DATA_PUT: {
                                response = repo.put(message.getKey(), message.getValue());
                                break;
                            }

                            default:
                                response = new KVMessage(message.getKey(), null, IKVMessage.StatusType.FAILED);
                        }
                    } else {
                        switch (servermessage.getStatus()) {
                            case SERVER_START: {
                                server.updateServerStopped(false);
                                serverresponse = new ServerMessage("", "", IServerMessage.StatusType.SERVER_START);
                                break;
                            }
                            case SERVER_STOP: {
                                server.updateServerStopped(false);
                                response = new KVMessage("", "", IKVMessage.StatusType.SERVER_STOPPED);
                                break;
                            }
                            case SERVER_LOCK_WRITE: {
                                server.lockWrite();
                                // TODO: convert these into success and failure message (although idk how it could fail)
                                response = new KVMessage("", "", IKVMessage.StatusType.SERVER_WRITE_LOCK);
                                break;
                            }
                            case SERVER_UNLOCK_WRITE: {
                                server.unLockWrite();
                                serverresponse = new ServerMessage("", "", IServerMessage.StatusType.SERVER_WRITE_UNLOCKED);
                                break;
                            }
                            case SERVER_MOVE_DATA: {
                                // I'm storing the json of the metadata in the key field of the KVMessage lmao
                                ECSNode metadata = new Gson().fromJson(message.getKey(), ECSNode.class);
                                server.moveData(metadata);
                                serverresponse = new ServerMessage("", "", IServerMessage.StatusType.SERVER_MOVE_DATA);
                                break;
                            }
                            case SERVER_UPDATE_METADATA: {
                                // metadata also stored in the key
                                ECSNode metadata = new Gson().fromJson(message.getKey(), ECSNode.class);
                                server.updateMetadata(metadata);
                                serverresponse = new ServerMessage("", "", IServerMessage.StatusType.SERVER_UPDATE_METADATA);
                                break;
                            }
                            case SERVER_UPDATE_ALL_METADATA: {
                                AllServerMetadata asm = new Gson().fromJson(message.getKey(), AllServerMetadata.class);
                                server.replaceAllServerMetadata(asm);
                                serverresponse = new ServerMessage("", "", IServerMessage.StatusType.SERVER_UPDATE_ALL_METADATA);
                                break;
                            }
                            default:
                                serverresponse = new ServerMessage(message.getKey(), null, IServerMessage.StatusType.FAILED);
                        }
                    }
                    if (response != null) {
                        messenger.sendMessage(new TextMessage(response));
                    } else {
                        messenger.sendMessage(new TextMessage(serverresponse));
                    }
                    /* connection either terminated by the client or lost due to
                     * network problems*/
                } catch (IOException ioe) {
                    logger.info("Error! Connection lost or client closed connection!", ioe);
                    isOpen = false;
                }
            }
        }finally {
            messenger.closeConnections();
        }
    }
}
