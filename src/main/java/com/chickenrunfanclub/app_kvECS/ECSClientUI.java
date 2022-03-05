package com.chickenrunfanclub.app_kvECS;

import com.chickenrunfanclub.app_kvServer.IKVServer;
import com.chickenrunfanclub.logger.LogSetup;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class ECSClientUI {

    private static Logger logger = LogManager.getLogger(ECSClientUI.class);
    private static final String PROMPT = "ECSClient> ";
    private BufferedReader stdin;
    private ECSClient ecsClient = null;
    private boolean stop = false;
    private String config_file = null;
    private String cacheStrategy;
    private int cacheSize;

    public ECSClientUI(String config_file, int cacheSize, String strategy) {
        this.config_file = config_file;
        this.cacheSize = cacheSize;
        try {
            IKVServer.CacheStrategy.valueOf(strategy);
        } catch (IllegalArgumentException e) {
            logger.error("Error! Unknown cache strategy");
            return;
        }
        this.cacheStrategy = strategy;
        try {
            ecsClient = new ECSClient(this.config_file, this.cacheStrategy, this.cacheSize);
//            ecsClient.start();
        } catch(Exception e){
            e.printStackTrace();
            logger.error("Error! Could not initialize ECS");
        }
    }

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

    private void start(String config_file, String cacheStrategy, int cacheSize) {
        try {
            ecsClient = new ECSClient(config_file, cacheStrategy, cacheSize);
            ecsClient.start();
        } catch (Exception e) {
            logger.info("Error! Could not initialize ECS");
        }
    }

    private void handleCommand(String cmdLine) {
        if (cmdLine == null) {
            return;
        }

        String[] tokens = cmdLine.split("\\s+");

        if (tokens[0].equals("shutdown")) {
            stop = true;
            shutdown();
            System.out.println(PROMPT + "Application exit!");

        } else if (tokens[0].equals("stop")) {
            stop();
            System.out.println(PROMPT + "Application stopped!");

        } else if (tokens[0].equals("add")) {
            if (tokens.length >= 2) {
                try {
                    int num_nodes = Integer.parseInt(tokens[1]);
                    if (ecsClient != null && ecsClient.isRunning()) {
                        addNodes(num_nodes);
                    } else {
                        printError("ECSClient is not running");
                    }
                } catch (Exception e) {
                    printError("Number of nodes must be an integer");
                }
            } else {
                if (ecsClient != null && ecsClient.isRunning()) {
                    addNode();
                } else {
                    printError("ECSClient is not running");
                }
            }

        } else if (tokens[0].equals("start")) {
            if (ecsClient.isRunning()) {
                printError("ECSClient is already running");
            } else {
                start(config_file, cacheStrategy, cacheSize);
            }

        } else if (tokens[0].equals("remove")) {
            if (tokens.length == 2) {
                try {
                    int node_idx = Integer.parseInt(tokens[1]);
                    if (ecsClient != null && ecsClient.isRunning()) {
                        removeNode(node_idx);
                    } else {
                        printError("ECSClient is not running");
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


    private void shutdown() {
        try {
            if (ecsClient != null) {
                ecsClient.shutdown();
                ecsClient = null;
            }
        } catch (Exception e) {
            printError("Could not shutdown");
        }
    }

    private void stop() {
        try {
            if (ecsClient != null) {
                ecsClient.stop();
            }
        } catch (Exception e) {
            printError("Could not stop application");
        }
    }

    private void addNode(){
        try {
            if (ecsClient != null) {
                ecsClient.addNode(cacheStrategy, cacheSize);
                System.out.println(PROMPT + "Node added successfully");
            }
        } catch (Exception e) {
            printError("Node addition unsuccessful");
        }
    }

    private void addNodes(int num_nodes){
        try {
            if (ecsClient != null) {
                ecsClient.addNodes(num_nodes, cacheStrategy, cacheSize);
                logger.info("Nodes Added");
                System.out.println(PROMPT + "Nodes added successfully");
            }
        } catch (Exception e) {
            printError("Node additions unsuccessful");
        }
    }

    private void removeNodes(Collection<String> nodes){
        try {
            if (ecsClient != null) {
                ecsClient.removeNodes(nodes);
                logger.info("Node Removed");
                System.out.println(PROMPT + "Node removed successfully");
            }
        } catch (Exception e) {
            printError("Node removal unsuccessful");
        }
    }

    private void removeNode(int nodeIdx){
        try {
            if (ecsClient != null) {
                ecsClient.removeNode(nodeIdx);
                logger.info("Node Removed");
                System.out.println(PROMPT + "Node removed successfully");
            }
        } catch (Exception e) {
            printError("Node removal unsuccessful");
        }
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECS CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("shutdown");
        sb.append("\t Shutsdown all servers and exits the application\n");
        sb.append(PROMPT).append("stop");
        sb.append("\t\t Stops all servers \n");
        sb.append(PROMPT).append("start");
        sb.append("\t\t Restart all servers\n");
        sb.append(PROMPT).append("add <num_nodes>");
        sb.append("\t\t\t Adds given number of nodes. If no number is specified adds 1 \n");
        sb.append(PROMPT).append("remove <nodes>");
        sb.append("\t\t\t Removes given nodes, any number of nodes can be given \n");
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
