package com.chickenrunfanclub.app_kvClient;

import com.chickenrunfanclub.client.KVCommInterface;
import com.chickenrunfanclub.client.KVStore;
import com.chickenrunfanclub.logger.LogSetup;
import com.chickenrunfanclub.shared.messages.IKVMessage;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

public class KVClient implements IKVClient {

    private static Logger logger = LogManager.getLogger(KVClient.class);
    private static final String PROMPT = "KVClient> ";
    private BufferedReader stdin;
    private KVStore kvStore = null;
    private boolean stop = false;

    private String serverAddress;
    private int serverPort;

    public void run() {
        while (!stop) {
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
        if (cmdLine == null) {
            return;
        }

        String[] tokens = cmdLine.split("\\s+");

        if (tokens[0].equals("quit")) {
            stop = true;
            disconnect();
            System.out.println(PROMPT + "Application exit!");

        } else if (tokens[0].equals("connect")) {
            if (tokens.length == 3) {
                try {
                    serverAddress = tokens[1];
                    serverPort = Integer.parseInt(tokens[2]);
                    newConnection(serverAddress, serverPort);
                    logger.info("Created new connection at " + serverAddress + ":" + serverPort);
                } catch (NumberFormatException nfe) {
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

        } else if (tokens[0].equals("put")) {
            if (tokens.length >= 2) {
                if (kvStore != null && kvStore.isRunning()) {
                    StringBuilder value = new StringBuilder();
                    if (tokens.length > 2) {
                        for (int i = 2; i < tokens.length; i++) {
                            value.append(tokens[i]);
                            if (i != tokens.length - 1) {
                                value.append(" ");
                            }
                        }
                        handlePut(tokens[1], value.toString());
                    } else {
                        handleDelete(tokens[1]);
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

        } else if (tokens[0].equals("disconnect")) {
            disconnect();
            System.out.println(PROMPT + "Application exit!");

        } else if (tokens[0].equals("logLevel")) {
            if (tokens.length == 2) {
                String level = setLevel(tokens[1]);
                if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
                    printError("No valid log level!");
                    printPossibleLogLevels();
                } else {
                    System.out.println(PROMPT +
                            "Log level changed to level " + level);
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("help")) {
            printHelp();
        } else {
            printError("Unknown command");
            printHelp();
        }
    }

    private void handlePut(String key, String value) {
        try {
            IKVMessage putMessage = kvStore.put(key, value);
            logger.info("Put has completed.");
            IKVMessage.StatusType status = putMessage.getStatus();
            if (status == IKVMessage.StatusType.PUT_SUCCESS) {
                System.out.println(PROMPT + "Key Value Insertion successful");
            } else if (status == IKVMessage.StatusType.PUT_UPDATE) {
                System.out.println(PROMPT + "Key Value Update successful");
            } else if (status == IKVMessage.StatusType.PUT_ERROR) {
                printError("Update/Insertion Unsuccessful!");
            } else {
                printError("Unable to execute command!");
                disconnect();
            }
        } catch (Exception e) {
            printError("Unable to execute command!");
            disconnect();
        }
    }

    private void handleDelete(String key) {
        try {
            IKVMessage delMessage = kvStore.put(key, null);
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


    private void get(String key) {
        try {
            IKVMessage getMessage = kvStore.get(key);
            String value = getMessage.getValue();
            IKVMessage.StatusType status = getMessage.getStatus();
            if (status == IKVMessage.StatusType.GET_SUCCESS) {
                System.out.println(value);
            } else if (status == IKVMessage.StatusType.GET_ERROR) {
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
        kvStore = new KVStore(hostname, port);
        try {
            kvStore.connect();
        } catch (Exception e) {
            logger.info("Error! Could not establish connection");
        }
    }

    private void disconnect() {
        if (kvStore != null) {
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
        if (levelString.equals(Level.ALL.toString())) {
            Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.ALL);
            return Level.ALL.toString();
        } else if (levelString.equals(Level.DEBUG.toString())) {
            Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.DEBUG);
            return Level.DEBUG.toString();
        } else if (levelString.equals(Level.INFO.toString())) {
            Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.INFO);
            return Level.INFO.toString();
        } else if (levelString.equals(Level.WARN.toString())) {
            Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.WARN);
            return Level.WARN.toString();
        } else if (levelString.equals(Level.ERROR.toString())) {
            Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.ERROR);
            return Level.ERROR.toString();
        } else if (levelString.equals(Level.FATAL.toString())) {
            Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.FATAL);
            return Level.FATAL.toString();
        } else if (levelString.equals(Level.OFF.toString())) {
            Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    @Override
    public KVCommInterface getStore() {
        // TODO Auto-generated method stub
        return kvStore;
    }
}
