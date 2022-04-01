package com.chickenrunfanclub.shared.messages;

public interface IServerMessage {

    public enum StatusType {
        SERVER_START,                /* Start Server, all requests are processed */
        SERVER_STOP,                /* Stop server, no requests are processed */
        SERVER_WRITE_UNLOCKED,        /* Unlock Write, write requests are processed */
        SERVER_MOVE_DATA,            /* Move data between servers based on host and port */
        SERVER_UPDATE_METADATA,        /* Server metadata is updated */
        SERVER_LOCK_WRITE,            /* Lock writes to the server */
        SERVER_UNLOCK_WRITE,        /* Unlock writes to the server */
        SERVER_SHUTDOWN,            /* Server is to be shutdown all nodes to be killed */
        FAILED,                      /* Operation failed */
        SERVER_UPDATE_ALL_METADATA,
        SERVER_HEARTBEAT;
    }

    /**
     * @return the key that is associated with this message,
     * null if not key is associated.
     */
    public String getKey();

    /**
     * @return the value that is associated with this message,
     * null if not value is associated.
     */
    public String getValue();

    /**
     * @return a status string that is used to identify request types,
     * response types and error types associated to the message.
     */
    public IServerMessage.StatusType getStatus();

    /**
     * @return a string in the form `status key value` to be used in message passing
     */
    public String toString();
}
