package com.chickenrunfanclub.shared.messages;

public interface IAuthMessage {

    public enum StatusType {
        ADD,                         /* Request to add username password pair */
        ADD_SUCCESS,                 /* username password pair added successfully */
        ADD_ERROR,                   /* username password pair could not be added */
        PASSWORD_UPDATE,             /* Updated a user's password */
        AUTH,                        /* Request to authenticate */
        AUTH_SUCCESS,                /* Authentification successful */
        AUTH_ERROR,                  /* Authentification unsuccessful */
        FAILED,                      /* Operation failed */
        DELETE,                      /* Delete a user */
        GET_SUCCESS,
        GET_ERROR,
        DELETE_SUCCESS,
        DELETE_ERROR
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
    public IAuthMessage.StatusType getStatus();

    /**
     * @return a string in the form `status key value` to be used in message passing
     */
    public String toString();
}

