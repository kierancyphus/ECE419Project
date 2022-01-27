package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import client.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logging.LogSetup;
import shared.messages.IKVMessage;
import org.w3c.dom.Text;
import ui.Application;

public class KVClient implements IKVClient{

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "KVClient> ";
    private BufferedReader stdin;
    private KVStore kvStore = null;
    private boolean stop = false;

    private String serverAddress;
    private int serverPort;

    public void run() {
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
            }
        }
    }

    private void handleCommand(String cmdLine) {
        // TODO Figure out if splitting by all whitespace is ok or if we should like be more careful since when we parse it back we just add spaces and like that's def not ideal and jsut not good
        String[] tokens = cmdLine.split("\\s+");

        if(tokens[0].equals("quit")) {
            stop = true;
            disconnect();
            System.out.println(PROMPT + "Application exit!");

        } else if (tokens[0].equals("connect")){
            if(tokens.length == 3) {
                try{
                    serverAddress = tokens[1];
                    serverPort = Integer.parseInt(tokens[2]);
                    newConnection(serverAddress, serverPort);
                } catch(NumberFormatException nfe) {
                    printError("No valid address. Port must be a number!");
                    logger.info("Unable to parse argument <port>", nfe);
                } catch (UnknownHostException e) {
                    printError("Unknown Host!");
                    logger.info("Unknown Host!", e);
                } catch (IOException e) {
                    printError("Could not establish connection!");
                    logger.warn("Could not establish connection!", e);
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else  if (tokens[0].equals("put")) {
            if (tokens.length >= 2) {
                if (kvStore != null && kvStore.isRunning()) {
                    StringBuilder value = new StringBuilder();
                    if (tokens.length == 2){
                        for (int i = 2; i < tokens.length; i++) {
                            value.append(tokens[i]);
                            if (i != tokens.length - 1) {
                                value.append(" ");
                            }
                        }
                        put(tokens[1], value.toString());
                    } else {
                        delete(tokens[1]);
                    }
                } else {
                    printError("Not connected!");
                }
            } else {
                printError("Missing key and value!");
            }

        } else if (tokens[0].equals("get")) {
            if (tokens.length == 2) {
                if (kvStore != null && kvStore.isRunning()) {
                    get(tokens[1]);
                } else {
                    printError("Not connected!");
                }
            } else if (tokens.length == 1) {
                printError("No key passed!");
            } else {
                printError("Too many arguments!");
            }

        } else if(tokens[0].equals("disconnect")) {
            disconnect();
            System.out.println(PROMPT + "Application exit!");

        } else if(tokens[0].equals("logLevel")) {
            if(tokens.length == 2) {
                String level = setLevel(tokens[1]);
                if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
                    printError("No valid log level!");
                    printPossibleLogLevels();
                } else {
                    System.out.println(PROMPT +
                            "Log level changed to level " + level);
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else if(tokens[0].equals("help")) {
            printHelp();
        } else {
            printError("Unknown command");
            printHelp();
        }
    }

    private void put(String key, String value){
        try {
            IKVMessage putMessage = kvStore.put(key, value);
            IKVMessage.StatusType status = putMessage.getStatus();
            if (status == IKVMessage.StatusType.PUT_SUCCESS) {
                System.out.println(PROMPT + "Key Value Insertion successful")
            }
            else if (status == IKVMessage.StatusType.PUT_UPDATE) {
                System.out.println(PROMPT + "Key Value Update successful")
            }
            else if (status == IKVMessage.StatusType.PUT_ERROR) {
                printError("Update/Insertion Unsuccessful!");
            }
            else {
                printError("Unable to execute command!");
                disconnect();
            }
        } catch (Exception e) {
            printError("Unable to execute command!");
            disconnect();
        }
    }

    private void delete(String key) {
        try {
            IKVMessage delMessage = kvStore.delete(key);
            IKVMessage.StatusType status = delMessage.getStatus();
            if (status == IKVMessage.StatusType.DELETE_SUCCESS) {
                System.out.println(PROMPT + "Key Deletion successful");
            } else if (status == IKVMessage.StatusType.DELETE_ERROR) {
                printError("Key Deletion Unsuccessful!");
            } else {
                printError("Unable to execute command!");
                disconnect();
            }
        } catch (Exception e) {
            printError("Unable to execute command!");
            disconnect();
        }
    }


    private void get(String key){
        try {
            IKVMessage getMessage = kvStore.get(key);
            String value = getMessage.getValue();
            IKVMessage.StatusType status = getMessage.getStatus();
            if (status == IKVMessage.StatusType.GET_SUCCESS){
                System.out.println(PROMPT + value);
            } else if (status == IKVMessage.StatusType.GET_ERROR){
                printError("Get Unsuccessful!");
            } else {
                printError("Unable to execute command!");
            }
        } catch (Exception e) {
            printError("Unable to execute command!");
            disconnect();
        }
    }
    @Override
    public void newConnection(String hostname, int port) throws IOException, UnknownHostException {
        // TODO Auto-generated method stub
        kvStore = new KVStore(hostname, port);
        kvStore.addListener(this);
        kvStore.start();
    }

    private void disconnect() {
        if(kvStore != null) {
            kvStore.closeConnection();
            kvStore = null;
        }
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("KEY VALUE CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("put <key> <value>");
        sb.append("\t\t inserts updates a key value pair, If no value is given the key is deleted \n");
        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t gets corresponding value of a key\n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");
        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t exits the program");
        System.out.println(sb.toString());
    }

    private void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    private String setLevel(String levelString) {

        if(levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if(levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if(levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if(levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if(levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if(levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if(levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    public void handleNewMessage(TextMessage msg) {
        if(!stop) {
            System.out.println(msg.getMsg());
            System.out.print(PROMPT);
        }
    }

    public void handleStatus(ClientSocketListener.SocketStatus status) {
        if(status == ClientSocketListener.SocketStatus.CONNECTED) {

        } else if (status == ClientSocketListener.SocketStatus.DISCONNECTED) {
            System.out.print(PROMPT);
            System.out.println("Connection terminated: "
                    + serverAddress + " / " + serverPort);

        } else if (status == ClientSocketListener.SocketStatus.CONNECTION_LOST) {
            System.out.println("Connection lost: "
                    + serverAddress + " / " + serverPort);
            System.out.print(PROMPT);
        }

    }

    private void printError(String error){
        System.out.println(PROMPT + "Error! " +  error);
    }

    @Override
    public KVCommInterface getStore() {
        // TODO Auto-generated method stub
        return kvStore;
    }

    /**
     * Main entry point for the KV client
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.OFF);
            KVClient cli = new KVClient();
            cli.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
