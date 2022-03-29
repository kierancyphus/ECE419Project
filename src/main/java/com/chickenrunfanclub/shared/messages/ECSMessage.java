package com.chickenrunfanclub.shared.messages;

import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ECSMessage implements IECSMessage {
    private String key;
    private String value;
    private IECSMessage.StatusType status;
    private final static Logger logger = LogManager.getLogger(ECSMessage.class);

    public ECSMessage(String key, String value, IECSMessage.StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
    }

    public ECSMessage(TextMessage textMessage) {
        ECSMessage message = new Gson().fromJson(textMessage.getMsg().trim(), ECSMessage.class);
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
