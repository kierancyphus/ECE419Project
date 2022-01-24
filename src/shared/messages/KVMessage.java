package shared.messages;

import java.io.Serializable;

// TODO: this needs to implement serializable
public class KVMessage implements IKVMessage, Serializable {
    String key;
    String value;
    IKVMessage.StatusType status;

    public KVMessage(String key, String value, IKVMessage.StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
    }

    @Override
    public String getKey() {
        return this.key;
    }

    @Override
    public String getValue() {
        return this.value;
    }

    @Override
    public StatusType getStatus() {
        return this.status;
    }

    /**
     * Used to get serialized message content
     * */
    public String toString() {
        return this.getStatus() + " " + this.getKey() + " " + this.getValue();
    }
}
