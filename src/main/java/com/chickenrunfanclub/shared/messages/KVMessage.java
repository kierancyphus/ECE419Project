package com.chickenrunfanclub.shared.messages;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KVMessage implements IKVMessage {
    private String key;
    private String value;
    private IKVMessage.StatusType status;
    private final static Logger logger = LogManager.getLogger(KVMessage.class);

    public KVMessage(String key, String value, IKVMessage.StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
    }

    public KVMessage(TextMessage textMessage) {
        String[] tokens = textMessage.getMsg().trim().split(" ");
        this.status = IKVMessage.StatusType.valueOf(tokens[0]);
        this.key = tokens[1];
        StringBuilder val = new StringBuilder();
        if (tokens.length > 3) {
            for (int i = 2; i < tokens.length; i++) {
                val.append(tokens[i]);
                if (i != tokens.length - 1) {
                    val.append(" ");
                }
            }
            this.value = val.toString();
        }
        else {
            this.value = tokens[2].equals("null") ? null : tokens[2];
        }
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
