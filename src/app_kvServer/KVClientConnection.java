package app_kvServer;

import org.apache.log4j.Logger;
import shared.messages.TextMessage;
import shared.Messenger;
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

    private static final Logger logger = Logger.getRootLogger();
    private final Messenger messenger;
    private final KVRepo repo;
    private boolean isOpen;

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     *
     * @param clientSocket the Socket object for the client connection.
     */
    public KVClientConnection(Socket clientSocket, KVRepo repo) {
        logger.info("Initializing the Client connection with " + clientSocket.getPort());
        this.isOpen = true;
        this.messenger = new Messenger(clientSocket);
        this.repo = repo;
    }

    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try {

            IKVMessage message;
            IKVMessage response;

            while (isOpen) {
                try {
                    TextMessage latestMsg = messenger.receiveMessage();

                    message = new KVMessage(latestMsg);

                    if (message.getStatus() == IKVMessage.StatusType.GET) {
                        response = repo.get(message.getKey());
                    } else if (message.getStatus() == IKVMessage.StatusType.PUT) {
                        response = repo.put(message.getKey(), message.getValue());
                    } else {
                        response = new KVMessage(message.getKey(), null, IKVMessage.StatusType.FAILED);
                    }
                    messenger.sendMessage(new TextMessage(response));

                /* connection either terminated by the client or lost due to
                 * network problems*/
                } catch (IOException ioe) {
                    logger.error("Error! Connection lost!", ioe);
                    isOpen = false;
                }
            }

        } finally {
            messenger.closeConnections();
        }
    }
}
