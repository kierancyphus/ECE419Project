package com.chickenrunfanclub.app_kvECS;

import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.shared.Messenger;
import com.chickenrunfanclub.shared.messages.*;
import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Socket;

public class ECSClientConnection implements Runnable {

    private static final Logger logger = LogManager.getLogger(ECSClientConnection.class);
    private final Messenger messenger;
    private final ECSClient ecs;
    private boolean isOpen;

    public ECSClientConnection(Socket clientSocket, ECSClient ecs) {
        logger.info("Initializing the Client connection with " + clientSocket.getPort());
        this.isOpen = true;
        this.messenger = new Messenger(clientSocket);
        this.ecs = ecs;
    }

    public void run() {
        try {

            IECSMessage message = null;
            IECSMessage response = null;
            IECSMessage serverresponse = null;

            while (isOpen) {
                try {
                    // sometimes the client sends weird empty messages, so this loop ensures we ignore those
                    TextMessage latestMsg = null;
                    while (latestMsg == null || latestMsg.getMsg().trim().length() < 1) {
                        latestMsg = messenger.receiveMessage();
                    }

                    message = new ECSMessage(latestMsg);
                    response = null;

                    switch (message.getStatus()) {
                        case ADD: {
                            try {
                                if (ecs.addNodes(Integer.parseInt(message.getKey())).size() == Integer.parseInt(message.getKey())) {
                                    response = new ECSMessage(message.getKey(), null, IECSMessage.StatusType.ADD_SUCCESS);
                                    break;
                                }
                                // response = (IECSMessage) ecs.addNodes(Integer.parseInt(message.getKey()));
                            } catch (Exception e) {
                                response = new ECSMessage(message.getKey(), null, IECSMessage.StatusType.FAILED);
                                break;
                            }
                        }
                        case REMOVE: {
                            try {
                                if (ecs.removeNode(Integer.parseInt(message.getKey()))) {
                                    response = new ECSMessage(message.getKey(), null, IECSMessage.StatusType.REMOVE_SUCCESS);
                                }
                            } catch (Exception e) {
                                response = new ECSMessage(message.getKey(), null, IECSMessage.StatusType.FAILED);
                            }

                            // response = ecs.removeNode(Integer.parseInt(message.getKey()));
                            break;
                        }
                        case ECS_START: {
                            try {
                                if (ecs.start()) {
                                    response = new ECSMessage(message.getKey(), null, IECSMessage.StatusType.ECS_START);
                                }
                            } catch (Exception e) {
                                logger.error(e);
                                response = new ECSMessage(message.getKey(), null, IECSMessage.StatusType.FAILED);
                            }
                            break;
                        }
                        case ECS_STOP: {
                            try {
                                if (ecs.stop()) {
                                    response = new ECSMessage(message.getKey(), null, IECSMessage.StatusType.ECS_STOPPED);
                                }
                            } catch (Exception e) {
                                response = new ECSMessage(message.getKey(), null, IECSMessage.StatusType.FAILED);
                            }
                            break;
                        }
                        case ECS_SHUTDOWN: {
                            try {
                                if (ecs.shutdown()) {
                                    response = new ECSMessage(message.getKey(), null, IECSMessage.StatusType.ECS_SHUTDOWN);
                                }
                            } catch (Exception e) {
                                response = new ECSMessage(message.getKey(), null, IECSMessage.StatusType.FAILED);
                            }
                            break;
                        }
                        default:
                            serverresponse = new ECSMessage(message.getKey(), null, IECSMessage.StatusType.FAILED);
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
        } finally {
            messenger.closeConnections();
        }
    }
}
