package com.chickenrunfanclub.shared.messages;

import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class APIMessage implements IAPIMessage {
    private String key;
    private String value;
    private String username;
    private String password;
    private IAPIMessage.StatusType status;
    private final static Logger logger = LogManager.getLogger(com.chickenrunfanclub.shared.messages.APIMessage.class);

    public APIMessage(String key, String value, IAPIMessage.StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
    }

    public APIMessage(TextMessage textMessage) {
        com.chickenrunfanclub.shared.messages.APIMessage message = new Gson().fromJson(textMessage.getMsg().trim(), com.chickenrunfanclub.shared.messages.APIMessage.class);
        this.key = message.getKey();
        this.value = message.getValue();
        this.status = message.getStatus();
        this.username = message.getUsername();
        this.password = message.getPassword();
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
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

