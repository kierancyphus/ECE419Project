package com.chickenrunfanclub.app_kvAPI;

import com.chickenrunfanclub.app_kvAuth.AuthClient;
import com.chickenrunfanclub.client.KVStore;
import com.chickenrunfanclub.shared.Messenger;
import com.chickenrunfanclub.shared.messages.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Socket;

public class APIConnection implements Runnable{
    private static final Logger logger = LogManager.getLogger(APIConnection.class);
    private final Messenger messenger;
    private final KVStore internal;
    private static AuthClient auth;
    private static KVAPIGateway gateway;
    private boolean isOpen;


    public APIConnection(KVStore internal, Socket clientSocket, AuthClient auth, KVAPIGateway gateway) {
        logger.info("Initializing the API connection with " + clientSocket.getPort());
        this.isOpen = true;
        this.messenger = new Messenger(clientSocket);
        this.internal = internal;
        this.auth = auth;
        this.gateway = gateway;
    }


    @Override
    public void run() {
        try{
            APIMessage message = null;
            APIMessage response = null;

            while (isOpen) {
                try {
                    // sometimes the client sends weird empty messages, so this loop ensures we ignore those
                    TextMessage latestMsg = null;
                    while (latestMsg == null || latestMsg.getMsg().trim().length() < 1) {
                        latestMsg = messenger.receiveMessage();
                    }

                    message = new APIMessage(latestMsg);
                    response = null;

                    switch (message.getStatus()) {
                        case ADD: {
//                            try {
//                                response = auth.add(message.getKey(), message.getValue());
//                                break;
//                            } catch (Exception e) {
//                                response = new APIMessage(message.getKey(), null, IAuthMessage.StatusType.FAILED);
//                                break;
//                            }
                        }
                        case AUTH: {

                        }
                        case DELETE: {

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
