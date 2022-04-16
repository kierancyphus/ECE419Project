package com.chickenrunfanclub.shared.messages;

import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AuthMessage implements IAuthMessage {
    private String key;
    private String value;
    private IAuthMessage.StatusType status;
    private final static Logger logger = LogManager.getLogger(com.chickenrunfanclub.shared.messages.AuthMessage.class);

    public AuthMessage(String key, String value, IAuthMessage.StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
    }

    public AuthMessage(TextMessage textMessage) {
        com.chickenrunfanclub.shared.messages.AuthMessage message = new Gson().fromJson(textMessage.getMsg().trim(), com.chickenrunfanclub.shared.messages.AuthMessage.class);
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

