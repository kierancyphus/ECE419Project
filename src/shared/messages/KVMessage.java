package shared.messages;

public class KVMessage implements IKVMessage {
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
}
