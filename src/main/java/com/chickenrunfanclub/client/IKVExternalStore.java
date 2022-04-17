package com.chickenrunfanclub.client;

import com.chickenrunfanclub.shared.messages.IKVMessage;

public interface IKVExternalStore {
    public IKVMessage get(String key);

    public IKVMessage put(String key, String value, int index);

}
