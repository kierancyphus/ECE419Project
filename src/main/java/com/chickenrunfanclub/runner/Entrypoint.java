package com.chickenrunfanclub.runner;

import com.chickenrunfanclub.app_kvClient.KVClient;
import com.chickenrunfanclub.app_kvECS.ECSClient;
import com.chickenrunfanclub.app_kvECS.ECSClientUI;
import com.chickenrunfanclub.app_kvServer.KVServer;
import com.chickenrunfanclub.logger.LogSetup;
import org.apache.logging.log4j.Level;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class Entrypoint {

    private static void runClient(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.OFF);
            if (args.length != 1) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: client <config_file>!");
            } else {
                String config_file = args[0];
                KVClient cli = new KVClient(config_file);
                cli.run();
            }

        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void runECS(String[] args) {
        try {
            new LogSetup("logs/ecs.log", Level.ALL);
            if (args.length != 4) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: ecs <config_file> <stategy> <cacheSize> <port>!");
            } else {
                String config_file = args[0];
                String cacheStrategy = args[1];
                int cacheSize = Integer.parseInt(args[2]);
                int port = Integer.parseInt(args[3]);
                ECSClient ecs = new ECSClient(config_file, cacheStrategy, cacheSize, port);
                ecs.run();
            }
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException nfe) {
            System.out.println("Error! Cache Size must be an integer");
            System.exit(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public static void runECSUI(String[] args) {
        try {
            new LogSetup("logs/ecs.log", Level.ALL);
            if (args.length != 0) {
                System.out.println("Error! Invalid number of arguments!");
            } else {
                ECSClientUI ecsUI = new ECSClientUI();
                ecsUI.run();
            }
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void runServer(String[] args) {
        try {
            new LogSetup("logs/server.log", Level.ALL);
            if (args.length != 3) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: Server <port> <cacheSize> <stategy>!");
            } else {
                int port = Integer.parseInt(args[0]);
                int cacheSize = Integer.parseInt(args[1]);
                String strategy = args[2];
                new KVServer(port, cacheSize, strategy).start();
                System.out.println("server started by ssh");
            }
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException nfe) {
            System.out.println("Error! Invalid argument <port>! Not a number!");
            System.out.println("Usage: Server <port>!");
            System.exit(1);
        }
    }

    /**
     * Main entry point for the m1 server application.
     *
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Error! Not specified if server or client");
            System.out.println("Usage: \"./gradlew run --args=\"server\"\" or \"./gradlew run --args=\"client\" --console=plain\"");
        } else if (Objects.equals(args[0], "server")) {
            runServer(Arrays.copyOfRange(args, 1, args.length));
        } else if (Objects.equals(args[0], "client")) {
            runClient(Arrays.copyOfRange(args, 1, args.length ));
        } else if (Objects.equals(args[0], "ecs")){
            runECS(Arrays.copyOfRange(args, 1, args.length));
        } else if (Objects.equals(args[0], "ecsUI")){
            runECSUI(Arrays.copyOfRange(args, 1, args.length));
        } else {
            System.out.println("Error! Not specified if server or client");
            System.out.println("Usage: \"./gradlew run --args=\"server\" \" or \"./gradlew run --args=\"client\" --console=plain\"");
        }
    }
}
