package com.chickenrunfanclub.app_kvServer;

import com.chickenrunfanclub.app_kvECS.AllServerMetadata;
import com.chickenrunfanclub.client.KVInternalStore;
import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.shared.messages.*;
import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.chickenrunfanclub.shared.Messenger;

import java.io.IOException;
import java.net.Socket;
import java.util.Objects;

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
            KVMessage message = null;
            IKVMessage response = null;
            IServerMessage servermessage = null;
            IServerMessage serverresponse = null;
            boolean KV;
            while (isOpen) {
                try {
                    boolean breaker = false;

                    // sometimes the client sends weird empty messages, so this loop ensures we ignore those
                    TextMessage latestMsg = null;
                    while (latestMsg == null || latestMsg.getMsg().trim().length() < 1) {
                        latestMsg = messenger.receiveMessage(server);
                        logger.info(latestMsg.getMsg());

                        if (!server.isRunning()) {
                            breaker = true;
                            break;
                        }

                        if (Objects.equals(latestMsg.getMsg(), "shutdown")) {
                            breaker = true;
                            break;
                        }
                    }
                    if (breaker) {
                        break;
                    }

                    message = new KVMessage(latestMsg);
                    servermessage = new ServerMessage(latestMsg);
                    KV = message.getStatus() != null;

                    response = null;
                    serverresponse = null;

                    if (KV) {
                        switch (message.getStatus()) {
                            case GET: {
                                // if we are in the replication chain
                                if (server.getAllMetadata().findGetServersResponsible(message.getKey()).contains(server.getMetadata())) {
                                    response = repo.get(message.getKey());
                                } else {
                                    String allServerMetadata = new Gson().toJson(server.getAllMetadata(), AllServerMetadata.class);
                                    response = new KVMessage(allServerMetadata, null, IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
                                }
                                break;
                            }
                            case PUT: {
                                // the first message was sent to the wrong head in the chain
                                if (server.getMetadata().notResponsibleFor(message.getKey()) && message.getIndex() == 0) {
                                    logger.debug("Not responsible for first message");
                                    String allServerMetadata = new Gson().toJson(server.getAllMetadata(), AllServerMetadata.class) ;
                                    response = new KVMessage(allServerMetadata, null, IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
                                }
                                // either index is 0 and responsible or passing along => persist to disk and then pass message along
                                else if (message.getIndex() >= 0 && message.getIndex() < 2) {
                                    // find next server in hash ring
                                    ECSNode nextServer = server.getAllMetadata().findServerAheadInHashRing(server.getMetadata(), 1);
                                    logger.debug("Passing message along to " + nextServer + " from " + server.getMetadata());


                                    // edge case here -> if we have a ring size of 1 this will be a deadlock because we are
                                    // trying to talk to ourselves. However, since this is also now the tail, we do nothing.
                                    logger.info("Next server is ourselves: " + nextServer.equals(server.getMetadata()));

                                    if (!nextServer.equals(server.getMetadata())) {
                                        // pass message along and increment index
                                        KVInternalStore client = new KVInternalStore(server.getAllMetadata());
                                        response = client.forwardPut(message.getKey(), message.getValue(), message.getIndex() + 1, nextServer.getHost(), nextServer.getPort());

                                        // persist to memory
                                        if (response.getStatus() != IKVMessage.StatusType.PUT_ERROR) {
                                            logger.debug(server.getMetadata() + " persisting");
                                            repo.put(message.getKey(), message.getValue());
                                        }
                                    } else {
                                        // ring size of 1 (just return persistence response)
                                        response = repo.put(message.getKey(), message.getValue());
                                        logger.info("ring size of 1");
                                        logger.info(response);
                                    }

                                }
                                // reached the end of the chain, so just persist to disk
                                else {
                                    logger.debug("Reached tail. Persisting.");
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
                        logger.debug("definitely a server message");

                        switch (servermessage.getStatus()) {
                            case SERVER_START: {
                                logger.debug("start");
                                server.updateServerStopped(false);
                                serverresponse = new ServerMessage("", "", IServerMessage.StatusType.SERVER_START);
                                break;
                            }
                            case SERVER_STOP: {
                                logger.debug("stop");
                                server.updateServerStopped(false);
                                response = new KVMessage("", "", IKVMessage.StatusType.SERVER_STOPPED);
                                break;
                            }
                            case SERVER_SHUTDOWN: {

                                logger.debug("shutdown");
                                serverresponse = new ServerMessage("", "", IServerMessage.StatusType.SERVER_SHUTDOWN);
                                break;
                            }
                            case SERVER_LOCK_WRITE: {

                                logger.debug("lockwrite");
                                server.lockWrite();
                                // TODO: convert these into success and failure message (although idk how it could fail)
                                response = new KVMessage("", "", IKVMessage.StatusType.SERVER_WRITE_LOCK);
                                break;
                            }
                            case SERVER_UNLOCK_WRITE: {

                                logger.debug("unlock");
                                server.unLockWrite();
                                serverresponse = new ServerMessage("", "", IServerMessage.StatusType.SERVER_WRITE_UNLOCKED);
                                break;
                            }
                            case SERVER_MOVE_DATA: {

                                logger.debug("move");
                                // I'm storing the json of the metadata in the key field of the KVMessage lmao
                                ECSNode metadata = new Gson().fromJson(message.getKey(), ECSNode.class);
                                server.moveData(metadata);
                                serverresponse = new ServerMessage("", "", IServerMessage.StatusType.SERVER_MOVE_DATA);
                                break;
                            }
                            case SERVER_UPDATE_METADATA: {

                                logger.debug("single metadata");
                                // metadata also stored in the key
                                ECSNode metadata = new Gson().fromJson(message.getKey(), ECSNode.class);
                                server.updateMetadata(metadata);
                                serverresponse = new ServerMessage("", "", IServerMessage.StatusType.SERVER_UPDATE_METADATA);
                                break;
                            }
                            case SERVER_UPDATE_ALL_METADATA: {
                                logger.info("updating all metadata \n\n\n\n\n\n\n");
                                AllServerMetadata asm = new Gson().fromJson(message.getKey(), AllServerMetadata.class);
                                server.replaceAllServerMetadata(asm);
                                serverresponse = new ServerMessage("", "", IServerMessage.StatusType.SERVER_UPDATE_ALL_METADATA);
                                break;
                            }
                            case SERVER_HEARTBEAT: {
                                String hbResponse = server.heartBeat();
                                serverresponse = new ServerMessage(hbResponse, "", IServerMessage.StatusType.SERVER_HEARTBEAT);
                                break;
                            }
                            default:
                                logger.debug("I am dumb");
                                serverresponse = new ServerMessage(message.getKey(), null, IServerMessage.StatusType.FAILED);
                        }
                    }
                    if (response != null) {
                        messenger.sendMessage(new TextMessage(response));
                    } else {
                        messenger.sendMessage(new TextMessage(serverresponse));
                        if (serverresponse.getStatus() == IServerMessage.StatusType.SERVER_SHUTDOWN) {
                            logger.info("About to kill the whole server");
                            server.kill();
                        }
                    }
                    /* connection either terminated by the client or lost due to
                     * network problems*/
                } catch (IOException ioe) {
                    logger.debug("Error! Connection lost or client closed connection!", ioe);
                    isOpen = false;
                }
            }
        } finally {
            logger.info("tearing down connectionss");
            messenger.closeConnections();
        }
    }
}
