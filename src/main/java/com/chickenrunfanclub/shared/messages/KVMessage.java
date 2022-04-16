package com.chickenrunfanclub.shared.messages;

import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KVMessage implements IKVMessage {
    private String key;
    private String value;
    private IKVMessage.StatusType status;
    private String username;
    private String password;
    private int index;
    private final static Logger logger = LogManager.getLogger(KVMessage.class);

    public KVMessage(String key, String value, IKVMessage.StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
        this.index = 0;
        username = null;
        password = null;
    }

    public KVMessage(String key, String value, IKVMessage.StatusType status, int index) {
        this.key = key;
        this.value = value;
        this.status = status;
        this.index = index;
        username = null;
        password = null;
    }

    public KVMessage(String key, String value, IKVMessage.StatusType status, int index, String username, String password) {
        this.key = key;
        this.value = value;
        this.status = status;
        this.index = index;
        this.username = username;
        this.password = password;
    }

    public KVMessage(TextMessage textMessage) {
        KVMessage message = new Gson().fromJson(textMessage.getMsg().trim(), KVMessage.class);
        this.key = message.getKey();
        this.value = message.getValue();
        this.status = message.getStatus();
        this.index = message.getIndex();
        this.username = message.getUsername();
        this.password = message.getPassword();
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

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
