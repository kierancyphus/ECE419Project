package com.chickenrunfanclub.app_kvAuth;

import com.chickenrunfanclub.shared.Messenger;
import com.chickenrunfanclub.shared.messages.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class AuthClient implements IAuthClient{

    private String address;
    private int port;
    private Messenger messenger;
    private Logger logger = LogManager.getLogger(AuthClient.class);
    private boolean running;
    private Socket clientSocket;

    public AuthClient() {
        String address = "localhost";
        int port = 50600;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean run) {
        running = run;
    }


    @Override
    public void connect(String address, int port) throws IOException, UnknownHostException {
        clientSocket = new Socket(address, port);
        messenger = new Messenger(clientSocket);
        setRunning(true);
        logger.info("Connection established");
    }

    @Override
    public void disconnect() {
        if (clientSocket != null) {
            messenger.closeConnections();
            clientSocket = null;
        }
    }

    public IAuthMessage sendAndReceiveMessage(String key, String value, IAuthMessage.StatusType status) throws Exception {
        connect("localhost", 50600);
        AuthMessage message = new AuthMessage(key, value, status);
        TextMessage textMessage = new TextMessage(message);
        messenger.sendMessage(textMessage);
        TextMessage response = messenger.receiveMessage();
        disconnect();
        return new AuthMessage(response);
    }

    @Override
    public IAuthMessage add(String username, String password) throws Exception {
        System.out.println("sending");
        return sendAndReceiveMessage(username, password, IAuthMessage.StatusType.ADD);
    }

    @Override
    public IAuthMessage authenticate(String username, String password) throws Exception {
        return sendAndReceiveMessage(username, password, IAuthMessage.StatusType.AUTH);
    }

    @Override
    public IAuthMessage delete(String username) throws Exception {
        return sendAndReceiveMessage(username, null, IAuthMessage.StatusType.DELETE);
    }

    public boolean isAuthenticated(String username, String password) {
        try {
            logger.info("I am here");
            IAuthMessage response = authenticate(username, password);
            logger.info(response);
            return response.getStatus() == IAuthMessage.StatusType.AUTH_SUCCESS;
        } catch (Exception e) {
            logger.debug(e);
            return false;
        }


    }
}
