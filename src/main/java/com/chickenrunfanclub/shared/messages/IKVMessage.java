package com.chickenrunfanclub.shared.messages;

public interface IKVMessage {

    public enum StatusType {
        GET,                        /* Get - request */
        GET_ERROR,                  /* requested tuple (i.e. value) not found */
        GET_SUCCESS,                /* requested tuple (i.e. value) found */
        PUT,                        /* Put - request */
        PUT_SUCCESS,                /* Put - request successful, tuple inserted */
        PUT_UPDATE,                 /* Put - request successful, i.e. value updated */
        PUT_ERROR,                  /* Put - request not successful */
        DELETE_SUCCESS,             /* Delete - request successful */
        DELETE_ERROR,               /* Delete - request successful */
        FAILED,                     /* General failure message - could not tell if put or get */

        SERVER_STOPPED,             /* Server is stopped, no requests are processed */
        SERVER_WRITE_LOCK,          /* Server locked for write, only get possible */
        SERVER_NOT_RESPONSIBLE,     /* Request not successful, server not responsible for key */

        MOVE_DATA_PUT,              /* This is used to move data without checking the metadata */

        NO_CREDENTIALS,             /* The user has not inputted a username or password yet */
        INVALID_CREDENTIALS,        /* The request was denied by the authentication service */

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
    public StatusType getStatus();

    /**
     * @return a string in the form `status key value` to be used in message passing
     */
    public String toString();

}


