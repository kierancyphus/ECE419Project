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
    private static AuthClient auth;
    private static KVAPIGateway gateway;
    private boolean isOpen;


    public APIConnection(Socket clientSocket, AuthClient auth, KVAPIGateway gateway) {
        logger.info("Initializing the API connection with " + clientSocket.getPort());
        this.isOpen = true;
        this.messenger = new Messenger(clientSocket);
        this.auth = auth;
        this.gateway = gateway;
    }


    @Override
    public void run() {
        try{
            APIMessage message = null;
            APIMessage apiResponse = null;
            KVMessage kvResponse = null;

            while (isOpen) {
                try {
                    // sometimes the client sends weird empty messages, so this loop ensures we ignore those
                    TextMessage latestMsg = null;
                    while (latestMsg == null || latestMsg.getMsg().trim().length() < 1) {
                        latestMsg = messenger.receiveMessage();
                    }

                    message = new APIMessage(latestMsg);
                    apiResponse = null;
                    kvResponse = null;
                    switch (message.getStatus()) {
                        case GET: {
                            try {
                                String username = message.getUsername();
                                String password = message.getPassword();
                                IAuthMessage auth_response = auth.authenticate(username, password);

                                if(auth_response.getStatus() != IAuthMessage.StatusType.AUTH_SUCCESS){
                                    apiResponse = new APIMessage(null, null, IAPIMessage.StatusType.AUTHENTICATION_FAILED);
                                }else {
                                    // TODO: How does the client actually access this?
                                    // TODO: How to implement different responses?
                                    kvResponse =  gateway.get();
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        case PUT: {
                            try {
                                String username = message.getUsername();
                                String password = message.getPassword();
                                IAuthMessage auth_response = auth.authenticate(username, password);

                                if(auth_response.getStatus() != IAuthMessage.StatusType.AUTH_SUCCESS){
                                    apiResponse = new APIMessage(null, null, IAPIMessage.StatusType.AUTHENTICATION_FAILED);
                                }else {
                                    // TODO: How does the client actually access this?
                                    // TODO: How to implement different responses?
                                    kvResponse =  gateway.put();
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    if (apiResponse != null) {
                        messenger.sendMessage(new TextMessage(kvResponse));
                    } else {
                        messenger.sendMessage(new TextMessage(apiResponse));
                    }
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
