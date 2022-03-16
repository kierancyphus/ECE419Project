package com.chickenrunfanclub.app_kvServer;

import com.chickenrunfanclub.app_kvECS.AllServerMetadata;
import com.chickenrunfanclub.client.KVStore;
import com.chickenrunfanclub.ecs.ECSNode;
import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.chickenrunfanclub.shared.messages.TextMessage;
import com.chickenrunfanclub.shared.Messenger;
import com.chickenrunfanclub.shared.messages.IKVMessage;
import com.chickenrunfanclub.shared.messages.KVMessage;

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

            KVMessage message;
            IKVMessage response;

            while (isOpen) {
                try {
                    // sometimes the client sends weird empty messages, so this loop ensures we ignore those
                    TextMessage latestMsg = null;
                    while (latestMsg == null || latestMsg.getMsg().trim().length() < 1) {
                        latestMsg = messenger.receiveMessage();
                    }

                    message = new KVMessage(latestMsg);

                    switch (message.getStatus()) {
                        case GET: {
                            if (server.getMetadata().notResponsibleFor(message.getKey())) {
                                // handle here
                                String allServerMetadata = new Gson().toJson(server.getAllMetadata(), AllServerMetadata.class) ;
                                response = new KVMessage(allServerMetadata, null, IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
                            } else {
                                response = repo.get(message.getKey());
                            }
                            break;
                        }
                        case PUT: {
                            // the first message was sent to the wrong head in the chain
                            if (server.getMetadata().notResponsibleFor(message.getKey()) && message.getIndex() == 0) {
                                logger.info("Not responsible for first message");
                                String allServerMetadata = new Gson().toJson(server.getAllMetadata(), AllServerMetadata.class) ;
                                response = new KVMessage(allServerMetadata, null, IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
                            }
                            // either index is 0 and responsible or passing along => persist to disk and then pass message along
                            else if (message.getIndex() >= 0 && message.getIndex() < 2) {
                                logger.info("persisting and passing message along");
                                // flush to memory -> this is also the response we send to the client
                                response = repo.put(message.getKey(), message.getValue());
                                // find next server in hash ring
                                ECSNode nextServer = server.getAllMetadata().findServerAheadInHashRing(server.getMetadata(), 1);

                                // edge case here -> if we have a ring size of 1 this will be a deadlock because we are
                                // trying to talk to ourselves. However, since this is also now the tail, we do nothing.
                                if (!nextServer.equals(server.getMetadata())) {
                                    // pass message along and increment index
                                    KVStore kvClient = new KVStore(nextServer.getHost(), nextServer.getPort());
                                    try {
                                        kvClient.connect();
                                        kvClient.put(message.getKey(), message.getValue(), message.getIndex() + 1);
                                    } catch (Exception e) {
                                        logger.info("Could not pass along message :(");
                                    }
                                }
                            }
                            // reached the end of the chain, so just persist to disk
                            else {
                                logger.info("Reached tail. Persisting.");
                                response = repo.put(message.getKey(), message.getValue());
                            }
                            break;

                        }
                        case SERVER_START: {
                            server.updateServerStopped(false);
                            response = new KVMessage("", "", IKVMessage.StatusType.SERVER_START);
                            break;
                        }
                        case SERVER_STOP: {
                            server.updateServerStopped(false);
                            response = new KVMessage("", "", IKVMessage.StatusType.SERVER_STOPPED);
                            break;
                        }
                        case SERVER_WRITE_LOCK: {
                            server.lockWrite();
                            // TODO: convert these into success and failure message (although idk how it could fail)
                            response = new KVMessage("", "", IKVMessage.StatusType.SERVER_WRITE_LOCK);
                            break;
                        }
                        case SERVER_WRITE_UNLOCKED: {
                            server.unLockWrite();
                            response = new KVMessage("", "", IKVMessage.StatusType.SERVER_WRITE_LOCK);
                            break;
                        }
                        case SERVER_MOVE_DATA: {
                            // I'm storing the json of the metadata in the key field of the KVMessage lmao
                            ECSNode metadata = new Gson().fromJson(message.getKey(), ECSNode.class);
                            server.moveData(metadata);
                            response = new KVMessage("", "", IKVMessage.StatusType.SERVER_MOVE_DATA);
                            break;
                        }
                        case SERVER_UPDATE_METADATA: {
                            // metadata also stored in the key
                            ECSNode metadata = new Gson().fromJson(message.getKey(), ECSNode.class);
                            server.updateMetadata(metadata);
                            response = new KVMessage("", "", IKVMessage.StatusType.SERVER_UPDATE_METADATA);
                            break;
                        }
                        default: response = new KVMessage(message.getKey(), null, IKVMessage.StatusType.FAILED);
                    }

                    messenger.sendMessage(new TextMessage(response));

                    /* connection either terminated by the client or lost due to
                     * network problems*/
                } catch (IOException ioe) {
                    logger.info("Error! Connection lost or client closed connection!", ioe);
                    isOpen = false;
                }
            }

        } finally {
            messenger.closeConnections();
        }
    }
}
