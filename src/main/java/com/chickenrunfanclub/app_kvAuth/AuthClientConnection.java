package com.chickenrunfanclub.app_kvAuth;

import com.chickenrunfanclub.shared.Messenger;
import com.chickenrunfanclub.shared.messages.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Socket;

public class AuthClientConnection implements Runnable {

    private static final Logger logger = LogManager.getLogger(com.chickenrunfanclub.app_kvECS.ECSClientConnection.class);
    private final Messenger messenger;
    private final AuthService auth;
    private boolean isOpen;

    public AuthClientConnection(Socket clientSocket, AuthService auth) {
        logger.info("Initializing the Client connection with " + clientSocket.getPort());
        this.isOpen = true;
        this.messenger = new Messenger(clientSocket);
        this.auth = auth;
    }

    public void run() {
        try {

            IAuthMessage message = null;
            IAuthMessage response = null;

            while (isOpen) {
                try {
                    // sometimes the client sends weird empty messages, so this loop ensures we ignore those
                    TextMessage latestMsg = null;
                    while (latestMsg == null || latestMsg.getMsg().trim().length() < 1) {
                        latestMsg = messenger.receiveMessage();
                    }

                    message = new AuthMessage(latestMsg);
                    response = null;

                    switch (message.getStatus()) {
                        case ADD: {
                            try {
                                response = auth.add(message.getKey(), message.getValue());
                                break;
                            } catch (Exception e) {
                                response = new AuthMessage(message.getKey(), null, IAuthMessage.StatusType.FAILED);
                                break;
                            }
                        }
                        case AUTH: {
                            try {
                                response = auth.authenticate(message.getKey(), message.getValue());
                            } catch (Exception e) {
                                response = new AuthMessage(message.getKey(), null, IAuthMessage.StatusType.FAILED);
                            }
                            break;
                        }
                        case DELETE: {
                            try {
                                response = auth.delete(message.getKey());
                            } catch (Exception e) {
                                response = new AuthMessage(message.getKey(), null, IAuthMessage.StatusType.FAILED);
                            }
                            break;
                        }
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
