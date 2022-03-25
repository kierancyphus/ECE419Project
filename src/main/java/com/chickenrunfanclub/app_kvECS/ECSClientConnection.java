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
                                // TODO: may need to return the nodelist in the message
                                String[] splited = message.getKey().split("\\s+");
                                if (ecs.addNodes(Integer.parseInt(splited[0]), splited[1], Integer.parseInt(splited[2])).size() == Integer.parseInt(splited[0])) {
                                    response = new ECSMessage(message.getKey(), null, IECSMessage.StatusType.ADD_SUCCESS);
                                }
                                // response = (IECSMessage) ecs.addNodes(Integer.parseInt(message.getKey()));
                            } catch (Exception e) {
                                response = new ECSMessage(message.getKey(), null, IECSMessage.StatusType.FAILED);
                            }
                        }
                        case REMOVE: {
                            try{
                                if (ecs.removeNode(Integer.parseInt(message.getKey()))){
                                    response = new ECSMessage(message.getKey(), null, IECSMessage.StatusType.REMOVE_SUCCESS);
                                }
                            } catch (Exception e) {
                                response = new ECSMessage(message.getKey(), null, IECSMessage.StatusType.FAILED);
                            }

                            // response = ecs.removeNode(Integer.parseInt(message.getKey()));
                        }
                        case ECS_START: {
                            try {
                                if (ecs.start()) {
                                    response = new ECSMessage(message.getKey(), null, IECSMessage.StatusType.ECS_START);
                                }
                            } catch (Exception e) {
                                response = new ECSMessage(message.getKey(), null, IECSMessage.StatusType.FAILED);
                            }
                        }
                        case ECS_STOP: {
                            try {
                                if (ecs.stop()) {
                                    response = new ECSMessage(message.getKey(), null, IECSMessage.StatusType.ECS_STOPPED);
                                }
                            } catch (Exception e) {
                                response = new ECSMessage(message.getKey(), null, IECSMessage.StatusType.FAILED);
                            }
                        }
                        case ECS_SHUTDOWN: {
                            try {
                                if (ecs.shutdown()) {
                                    response = new ECSMessage(message.getKey(), null, IECSMessage.StatusType.ECS_SHUTDOWN);
                                }
                            } catch (Exception e) {
                                response = new ECSMessage(message.getKey(), null, IECSMessage.StatusType.FAILED);
                            }
                        }
                        default:
                            response = new ECSMessage(message.getKey(), null, IECSMessage.StatusType.FAILED);
                    }
                    messenger.sendMessage(new TextMessage(response));
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
