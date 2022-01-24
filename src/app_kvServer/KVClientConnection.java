package app_kvServer;

import org.apache.log4j.Logger;
import server.TextMessage;
import shared.Messager;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     *
     * @param clientSocket the Socket object for the client connection.
     */
    public KVClientConnection(Socket clientSocket) {
        this.clientSocket = clientSocket;
        this.isOpen = true;
        this.messager = new Messager(clientSocket);
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

            while (isOpen) {
                try {
                    TextMessage latestMsg = messager.receiveMessage();

                    // TODO: IKVMessage message = parseMessage(...)
                    // if message.status = ...

                    messager.sendMessage(latestMsg);

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

}
