package com.chickenrunfanclub.app_kvAPI;

public interface IKVAPIGateway {
    public void run();

    public void kill();

    public void close();

    public String getKV(String key) throws Exception;

    public void putKV(String key, String value) throws Exception;

}