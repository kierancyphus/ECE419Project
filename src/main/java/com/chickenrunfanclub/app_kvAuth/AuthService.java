package com.chickenrunfanclub.app_kvAuth;

import com.chickenrunfanclub.app_kvECS.ECSClientConnection;
import com.chickenrunfanclub.shared.messages.AuthMessage;
import com.chickenrunfanclub.shared.messages.IAuthMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

import java.io.*;
import java.net.*;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class AuthService {
    private AuthRepo store;
    private static final Logger logger = LogManager.getLogger(com.chickenrunfanclub.app_kvECS.ECSClientConnection.class);
    private ServerSocket serverSocket;
    private int port;
    private boolean running = false;

    public AuthService(int port){
        store = new AuthRepo();
        this.port = port;
    }

    public IAuthMessage add(String user, String password){
        return store.put(user, password);
    }

    public IAuthMessage delete(String user){
        return store.put(user, null);
    }

    public IAuthMessage authenticate(String user, String password){
        IAuthMessage msg = store.get(user);
        System.out.println(msg.toString());
        System.out.println(msg.getValue());
        if (msg.getStatus() == IAuthMessage.StatusType.GET_SUCCESS){
            System.out.println("entered: "+password);
            System.out.println("found: "+msg.getValue());
            if (password.equals(msg.getValue())){
                return new AuthMessage(user, password, IAuthMessage.StatusType.AUTH_SUCCESS);
            }
            else  {
                logger.info("Password is wrong for the user");
                return new AuthMessage(user, password, IAuthMessage.StatusType.AUTH_ERROR);
            }
        }
        else {
            logger.info("User does not exist");
            return new AuthMessage(user, password, IAuthMessage.StatusType.AUTH_ERROR);
        }
    }

    private boolean initializeServer() {
        logger.info("Initialize Auth ...");
        try {
            serverSocket = new ServerSocket(port);
            logger.info("Auth listening on port: "
                    + serverSocket.getLocalPort());
            return true;

        } catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }

    public boolean isRunning() {
        return running;
    }

    public void stop() {
        this.running = false;
    }

    public void run() {
        running = initializeServer();

        if (serverSocket != null) {
            while (isRunning()) {
                try {
                    Socket client = serverSocket.accept();
                    AuthClientConnection connection = new AuthClientConnection(client, this);
                    new Thread(connection).start();
                    logger.info("Auth connected to "
                            + client.getInetAddress().getHostName()
                            + " on port " + client.getPort());
                } catch (IOException e) {
                    logger.error("Error! " +
                            "Unable to establish connection. \n", e);
                }
            }
        }
        logger.info("Server stopped.");
    }
}
