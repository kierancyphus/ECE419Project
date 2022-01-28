package shared.messages;

import org.apache.log4j.Logger;

public class KVMessage implements IKVMessage {
    private String key;
    private String value;
    private IKVMessage.StatusType status;
    private final static Logger logger = Logger.getRootLogger();

    public KVMessage(String key, String value, IKVMessage.StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
    }

    public KVMessage(TextMessage textMessage) {
        String[] tokens = textMessage.getMsg().trim().split(" ");
        this.status = IKVMessage.StatusType.valueOf(tokens[0]);
        this.key = tokens[1];
        this.value = tokens[2].equals("null") ? null : tokens[2];
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

    public String toString() {
        return this.getStatus() + " " + this.getKey() + " " + this.getValue();
    }
}
