package com.chickenrunfanclub.shared.messages;

import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ServerMessage implements IServerMessage {
    private String key;
    private String value;
    private IServerMessage.StatusType status;
    private final static Logger logger = LogManager.getLogger(ServerMessage.class);

    public ServerMessage(String key, String value, IServerMessage.StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
    }

    public ServerMessage(TextMessage textMessage) {
        ServerMessage message = new Gson().fromJson(textMessage.getMsg().trim(), ServerMessage.class);
        this.key = message.getKey();
        this.value = message.getValue();
        this.status = message.getStatus();
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
        return new Gson().toJson(this);
    }
}
