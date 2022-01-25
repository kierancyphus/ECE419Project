package app_kvServer;

import org.apache.log4j.Logger;
import shared.messages.TextMessage;
import shared.Messager;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

import java.io.IOException;
import java.net.Socket;

/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the client.
 */
public class KVClientConnection implements Runnable {

    private static Logger logger = Logger.getRootLogger();

    private boolean isOpen;

    private Socket clientSocket;
    private Messager messager;
    private KVRepo repo;

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     *
     * @param clientSocket the Socket object for the client connection.
     */
    public KVClientConnection(Socket clientSocket, KVRepo repo) {
        this.clientSocket = clientSocket;
        this.isOpen = true;
        this.messager = new Messager(clientSocket);
        this.repo = repo;
    }

    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try {
            messager.sendMessage(new TextMessage(
                    "Connection to KVStore server established: "
                            + clientSocket.getLocalAddress() + " / "
                            + clientSocket.getLocalPort()));

            IKVMessage message;
            IKVMessage response;

            while (isOpen) {
                try {
                    TextMessage latestMsg = messager.receiveMessage();

                    message = parseMessage(latestMsg.getMsg());

                    if (message.getStatus() == IKVMessage.StatusType.GET) {
                        response = repo.get(message.getKey());
                    } else if (message.getStatus() == IKVMessage.StatusType.PUT) {
                        response = repo.put(message.getKey(), message.getValue());
                    } else {
                        response = new KVMessage(message.getKey(), null, IKVMessage.StatusType.FAILED);
                    }

                    messager.sendMessage(new TextMessage(response));

                /* connection either terminated by the client or lost due to
                 * network problems*/
                } catch (IOException ioe) {
                    logger.error("Error! Connection lost!");
                    isOpen = false;
                }
            }

        } catch (IOException ioe) {
            logger.error("Error! Connection could not be established!", ioe);

        } finally {
            messager.closeConnections();
        }
    }

    private IKVMessage parseMessage(String message) {
        String[] parts = message.split(" ");
        IKVMessage kvMessage = null;
        try {
            String key = parts[1];
            String value = parts[2];
            IKVMessage.StatusType status = IKVMessage.StatusType.values()[Integer.parseInt(parts[0])];
            kvMessage = new KVMessage(key, value, status);
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.error("Error! Message could not be parsed. Make sure it is in the correct format.");
        } catch (NumberFormatException e) {
            logger.error("Error! Could not parse message status.");
        }
        return kvMessage;
    }

}
