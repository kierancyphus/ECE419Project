package com.chickenrunfanclub.shared;

import com.chickenrunfanclub.app_kvServer.KVClientConnection;
import com.chickenrunfanclub.app_kvServer.KVServer;
import com.chickenrunfanclub.shared.messages.TextMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
public class Messenger {

    private static Logger logger = LogManager.getLogger(Messenger.class);

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private Socket clientSocket;
    private InputStream input;
    private OutputStream output;


    /**
     * Constructs a new Connection object for a given TCP socket.
     *
     * @param socket the Socket object for the connection.
     */
    public Messenger(Socket socket) {
        this.clientSocket = socket;

        initializeStreams();
    }

    private void initializeStreams() {
        try {
            logger.debug("Attempting to initialize input and output");
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();

        } catch (IOException ioe) {
            logger.error("Error! Connection could not be established!", ioe);
        }
    }

    public void closeConnections() {
        try {
            logger.debug("Attempting to close all connections.");
            if (clientSocket != null) {
                input.close();
                output.close();
                clientSocket.close();
            }
        } catch (IOException ioe) {
            logger.error("Error! Unable to tear down connection!", ioe);
        }
    }

    /**
     * Method sends a TextMessage using this socket.
     *
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendMessage(TextMessage msg) throws IOException {
        byte[] msgBytes = msg.getMsgBytes();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.debug("SEND \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: '"
                + msg.getMsg() + "'");
    }


    public TextMessage receiveMessage() throws IOException {
        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

        /* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

        while (/*read != 13  && */ read != 10 && read != -1 && reading) {/* CR, LF, error */
            /* if buffer filled, copy to msg array */
            if (index == BUFFER_SIZE) {
                if (msgBytes == null) {
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[msgBytes.length + BUFFER_SIZE];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
                            BUFFER_SIZE);
                }

                msgBytes = tmp;
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            }

            /* only read valid characters, i.e. letters and constants */
            bufferBytes[index] = read;
            index++;

            /* stop reading is DROP_SIZE is reached */
            if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

            /* read next char from stream */
            read = (byte) input.read();
        }

        if (msgBytes == null) {
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else {
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }

        msgBytes = tmp;

        /* build final String */
        TextMessage msg = new TextMessage(msgBytes);
        logger.debug("RECEIVE \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: '"
                + msg.getMsg().trim() + "'");
        return msg;
    }

    public TextMessage receiveMessage(IRunning server) throws IOException {
        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

        /* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

        while (/*read != 13  && */ read != 10 && read != -1 && reading) {/* CR, LF, error */
//            logger.info("I think the server is running" + server.isRunning());

            if (!server.isRunning()) {
//                logger.info("The server has been shut down");
                return new TextMessage("shutdown");
            }

            /* if buffer filled, copy to msg array */
            if (index == BUFFER_SIZE) {
                if (msgBytes == null) {
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[msgBytes.length + BUFFER_SIZE];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
                            BUFFER_SIZE);
                }

                msgBytes = tmp;
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            }

            /* only read valid characters, i.e. letters and constants */
            bufferBytes[index] = read;
            index++;

            /* stop reading is DROP_SIZE is reached */
            if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

            /* read next char from stream */
            read = (byte) input.read();
        }

        if (msgBytes == null) {
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else {
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }

        msgBytes = tmp;

        /* build final String */
        TextMessage msg = new TextMessage(msgBytes);
        logger.debug("RECEIVE \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: '"
                + msg.getMsg().trim() + "'");
        return msg;
    }


}