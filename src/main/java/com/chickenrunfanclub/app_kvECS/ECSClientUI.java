package com.chickenrunfanclub.app_kvECS;

import com.chickenrunfanclub.app_kvServer.IKVServer;
import com.chickenrunfanclub.logger.LogSetup;
import com.chickenrunfanclub.shared.Messenger;
import com.chickenrunfanclub.shared.messages.*;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class ECSClientUI {
    private String address;
    private int port;

    private static Logger logger = LogManager.getLogger(ECSClientUI.class);
    private static final String PROMPT = "ECSClient> ";
    private BufferedReader stdin;
    private boolean running = false;
    private boolean connected = false;
    private boolean stop = false;
    private Socket clientSocket;
    private Messenger messenger;

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

    public IECSMessage sendAndReceiveECSMessage(IECSMessage.StatusType status) throws Exception {
        ECSMessage message = new ECSMessage(null, null, status);
        TextMessage textMessage = new TextMessage(message);
        messenger.sendMessage(textMessage);
        TextMessage response = messenger.receiveMessage();
        return new ECSMessage(response);
    }

    public IECSMessage start() throws Exception {
        return sendAndReceiveECSMessage(IECSMessage.StatusType.ECS_START);
    }

    public void connect(String address, int port) throws IOException, UnknownHostException {
        clientSocket = new Socket(address, port);
        messenger = new Messenger(clientSocket);
        connected = true;
        setRunning(true);
        logger.info("Connection established");
    }

    public void disconnect() {
        if (clientSocket != null) {
            messenger.closeConnections();
            clientSocket = null;
        }
        connected = false;
    }

    public void setRunning(boolean run) {
        running = run;
    }

    private void handleCommand(String cmdLine) {
        if (cmdLine == null) {
            return;
        }

        String[] tokens = cmdLine.split("\\s+");

        if (tokens[0].equals("shutdown")) {
            stop = true;
            try {
                shutdown();
            } catch (Exception e) {
                printError("Could not shutdown ECS");
            }
            System.out.println(PROMPT + "Application exit!");

        } else if (tokens[0].equals("stop")) {
            try {
                stop();
            } catch (Exception e) {
                printError("Could not stop ECS");
            }
            System.out.println(PROMPT + "Application stopped!");

        } else if (tokens[0].equals("connect")) {
                if (tokens.length == 3) {
                    try {
                        address = tokens[1];
                        port = Integer.parseInt(tokens[2]);
                        connect(address, port);
                        logger.info("Created new connection at " + address + ":" + port);
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
        } else if (tokens[0].equals("disconnect")) {
            disconnect();
            setRunning(false);
            System.out.println(PROMPT + "Application exit!");

        } else if (tokens[0].equals("add")) {
            if (tokens.length >= 2) {
                try {
                    int num_nodes = Integer.parseInt(tokens[1]);
                    if (running && connected) {
                        addNodes(num_nodes);
                    } else {
                        printError("ECSClient is not running or not connected");
                    }
                } catch (Exception e) {
                    printError("Number of nodes must be an integer");
                }
            } else {
                if (running && connected) {
                    addNode();
                } else {
                    printError("ECSClient is not running or not connected");
                }
            }

        } else if (tokens[0].equals("start")) {
            if (running) {
                printError("ECSClient is already running");
            } else {
                try {
                    start();
                } catch (Exception e) {
                    printError("Could not start ECS");
                }
            }

        } else if (tokens[0].equals("remove")) {
            if (tokens.length == 2) {
                try {
                    int node_idx = Integer.parseInt(tokens[1]);
                    if (running && connected) {
                        removeNode(node_idx);
                    } else {
                        printError("ECSClient is not running or not connected");
                    }
                } catch (Exception e) {
                    printError("Incorrect node index given.");
                }
//            if (tokens.length > 1) {
//                try {
//                    int node_idx = Integer.parseInt(tokens[1]);
//                    if (ecsClient != null && ecsClient.isRunning()) {
//                        List<String> nodes = Arrays.asList(Arrays.copyOfRange(tokens, 1, tokens.length));
//                        removeNodes(nodes);
//                    } else {
//                        printError("ECSClient is not running");
//                    }
//                } catch (Exception e) {
//                    printError("Incorrect nodes given, please give existing nodes.");
//                }
            } else {
                printError("Incorrect number of arguments, no node given");
            }

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


    private IECSMessage shutdown() throws Exception{
        running = false;
        return sendAndReceiveECSMessage(IECSMessage.StatusType.ECS_SHUTDOWN);
    }

    private IECSMessage stop() throws Exception{
        running = false;
        return sendAndReceiveECSMessage(IECSMessage.StatusType.ECS_STOP);
    }

    private void addNodes(int num_nodes){
        try {
            if (running && connected) {
                ECSMessage message = new ECSMessage(Integer.toString(num_nodes), null, IECSMessage.StatusType.ADD);
                TextMessage textMessage = new TextMessage(message);
                messenger.sendMessage(textMessage);
                TextMessage textResponse = messenger.receiveMessage();
                ECSMessage response = new ECSMessage(textResponse);
                logger.info("Add completed");

                IECSMessage.StatusType status  = response.getStatus();
                if (status == IECSMessage.StatusType.ADD_SUCCESS) {
                    System.out.println(PROMPT + "Node addition successful");
                } else if (status == IECSMessage.StatusType.ADD_ERROR) {
                    printError("Node addition Unsuccessful!");
                } else {
                    printError("Unable to execute command!");
                    disconnect();
                }
            }

        } catch (Exception e) {
            printError("Unable to execute command");
            disconnect();
        }
    }

    private void addNode(){
        addNodes(1);
    }

//    private void removeNodes(Collection<String> nodes){
//        try {
//            if (ecsClient != null) {
//                ecsClient.removeNodes(nodes);
//                logger.info("Node Removed");
//                System.out.println(PROMPT + "Node removed successfully");
//            }
//        } catch (Exception e) {
//            printError("Node removal unsuccessful");
//        }
//    }

    private void removeNode(int nodeIdx){
        try {
            if (running && connected) {
                ECSMessage message = new ECSMessage(Integer.toString(nodeIdx), null, IECSMessage.StatusType.REMOVE);
                TextMessage textMessage = new TextMessage(message);
                messenger.sendMessage(textMessage);
                TextMessage textResponse = messenger.receiveMessage();
                ECSMessage response = new ECSMessage(textResponse);
                logger.info("Removal completed");

                IECSMessage.StatusType status  = response.getStatus();
                if (status == IECSMessage.StatusType.REMOVE_SUCCESS) {
                    System.out.println(PROMPT + "Node removal successful");
                } else if (status == IECSMessage.StatusType.REMOVE_ERROR) {
                    printError("Node removal Unsuccessful!");
                } else {
                    printError("Unable to execute command!");
                    disconnect();
                }
            }

        } catch (Exception e) {
            printError("Unable to execute command");
            disconnect();
        }
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECS CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");
        sb.append(PROMPT).append("shutdown");
        sb.append("\t Shutsdown all servers and exits the application\n");
        sb.append(PROMPT).append("stop");
        sb.append("\t\t Stops all servers \n");
        sb.append(PROMPT).append("start");
        sb.append("\t\t Restart all servers\n");
        sb.append(PROMPT).append("add <num_nodes>");
        sb.append("\t\t\t Adds given number of nodes. If no number is specified adds 1 \n");
        sb.append(PROMPT).append("remove <node index>");
        sb.append("\t\t\t Removes the node at the given index \n");
        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
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

}
