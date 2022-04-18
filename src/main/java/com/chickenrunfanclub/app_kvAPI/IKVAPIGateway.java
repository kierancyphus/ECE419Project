package com.chickenrunfanclub.app_kvAPI;

public interface IKVAPIGateway {
    public void run();

    public void kill();

    public void close();

    public String get(String key) throws Exception;

    public void put(String key, String value) throws Exception;

}