package com.chickenrunfanclub.shared.messages;

public interface IECSMessage {

    public enum StatusType {
        ADD, 			/* Add - request */
        ADD_ERROR, 		/* Nodes added */
        ADD_SUCCESS, 	/* Nodes could not be added */
        REMOVE, 			/* Remove - request */
        REMOVE_SUCCESS, 	/* Node removal successful */
        REMOVE_UPDATE, 	/* Node removal failed */
        FAILED,			/* General failure message - could not tell if put or get */

        ECS_STOPPED,			/* ECS is stopped, no requests are processed */

        ECS_START,				/* Start ECS, */
        ECS_STOP,				/* Stop ECS */
        ECS_SHUTDOWN,           /* Shutdown ECS */
    }

    /**
     * @return the key that is associated with this message,
     * 		null if not key is associated.
     */
    public String getKey();

    /**
     * @return the value that is associated with this message,
     * 		null if not value is associated.
     */
    public String getValue();

    /**
     * @return a status string that is used to identify request types,
     * response types and error types associated to the message.
     */
    public StatusType getStatus();

    /**
     * @return a string in the form `status key value` to be used in message passing
     * */
    public String toString();

}


