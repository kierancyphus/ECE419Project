package com.chickenrunfanclub.apiGateway;

import com.chickenrunfanclub.client.KVInternalStore;
import com.chickenrunfanclub.shared.Messenger;
import com.chickenrunfanclub.shared.messages.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.util.Objects;

public class ApiGatewayClientConnection implements Runnable {

    private static final Logger logger = LogManager.getLogger(ApiGatewayClientConnection.class);
    private final Messenger messenger;
    private final ApiGateway server;
    private final KVInternalStore store;
    private boolean isOpen;

    public ApiGatewayClientConnection(Socket clientSocket, ApiGateway server) {
        this.isOpen = true;
        this.messenger = new Messenger(clientSocket);
        this.server = server;
        store = new KVInternalStore(server.getAllServerMetadata());
    }

    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try {

            KVMessage message;
            IKVMessage response;
            IServerMessage servermessage;
            IServerMessage serverresponse;
            boolean KV;

            while (isOpen) {
                try {
                    boolean breaker = false;

                    // sometimes the client sends weird empty messages, so this loop ensures we ignore those
                    TextMessage latestMsg = null;
                    while (latestMsg == null || latestMsg.getMsg().trim().length() < 1) {
                        latestMsg = messenger.receiveMessage(server);

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
                    if (KV) {
                        switch (message.getStatus()) {
                            case GET: {
                                // TODO: make call to auth service
                                // boolean authenticated = server.getAuthClient().isAuthenticated(message.getUsername(), message.getPassword());
                                boolean authenticated = true;
                                if (authenticated) {
                                    response = store.get(message.getKey());
                                } else {
                                    response = new KVMessage(message.getKey(), null, IKVMessage.StatusType.INVALID_CREDENTIALS);
                                }
                                break;
                            }
                            case PUT: {
                                // TODO: make call to auth service
                                // boolean authenticated = server.getAuthClient().isAuthenticated(message.getUsername(), message.getPassword());
                                boolean authenticated = true;
                                if (authenticated) {
                                    response = store.put(message.getKey(), message.getValue(), 0);
                                } else {
                                    response = new KVMessage(message.getKey(), message.getValue(), IKVMessage.StatusType.INVALID_CREDENTIALS);
                                }
                                // auth and then call
                                break;
                            }

                            default:
                                response = new KVMessage(message.getKey(), null, IKVMessage.StatusType.FAILED);
                        }
                    } else {
                        if (servermessage.getStatus() == IServerMessage.StatusType.SERVER_HEARTBEAT) {
                            String hbResponse = server.heartBeat();
                            serverresponse = new ServerMessage(hbResponse, "", IServerMessage.StatusType.SERVER_HEARTBEAT);
                            messenger.sendMessage(new TextMessage(serverresponse));
                        }
                    }

                    if (response != null) {
                        messenger.sendMessage(new TextMessage(response));
                    } else {
                        if (servermessage.getStatus() == IServerMessage.StatusType.SERVER_SHUTDOWN) {
                            messenger.sendMessage(new TextMessage(new ServerMessage(null, null, IServerMessage.StatusType.SERVER_SHUTDOWN)));
                            logger.info("About to kill the whole server");
                            server.close();
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
            messenger.closeConnections();
        }
    }
}
