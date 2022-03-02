package com.chickenrunfanclub.ecs;

public class ECSNode implements IECSNode{
    public enum ECSNodeFlag {
        STOP,
        START,
        SHUT_DOWN,
        UPDATE,
        ADDED,
        IDLE,
        IN_USE
    }

    private String name;
    private String host;
    private int port;
    private String hashStart;
    private String hashEnd;
    private String cacheStrategy;
    private int cacheSize;


    public ECSNode(String name, String host, int port, String hashEnd) {
        this.name = name;
        this.host = host;
        this.port = port;
        this.hashStart = "";
        this.hashEnd = hashEnd;
    }

    @Override
    public String getNodeName() {
        return this.name;
    }

    @Override
    public String getNodeHost() {
        return this.host;
    }

    @Override
    public int getNodePort() {
        return this.port;
    }

    @Override
    public String[] getNodeHashRange() {
        return new String[] {hashStart, hashEnd};
    }

    public void setHashStart(String val){
        hashStart = val;
    }

    public void setHashEnd(String val){
        hashEnd = val;
    }

    public String getServerHashVal(){
        return hashEnd;
    }

    public void setCacheStrategy(String val){
        cacheStrategy = val;
    }

    public String getCacheStrategy(){
        return cacheStrategy;
    }

    public void setCacheSize(int val){
        cacheSize = val;
    }

    public int getCacheSize(){
        return cacheSize;
    }
}
