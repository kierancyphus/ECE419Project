package com.chickenrunfanclub.ecs;

import com.chickenrunfanclub.shared.Hasher;

import java.util.Objects;

public class ECSNode implements IECSNode {
    public enum ECSNodeFlag {
        STOP,
        START,
        SHUT_DOWN,
        UPDATE,
        ADDED,
        IDLE,
        IN_USE
    }

    private String rangeStart;
    private String rangeEnd;
    private String host;
    private Integer port;
    private Boolean serverLock;
    private Boolean writeLock;
    private String cacheStrategy;
    private int cacheSize;
    private String name;
    private ECSNodeFlag status;

    public ECSNodeFlag getStatus() {
        return status;
    }

    public void setStatus(ECSNodeFlag status) {
        this.status = status;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ECSNode(String host, Integer port, String start, String end, Boolean serverLock, Boolean writeLock, String cacheStrategy, int cacheSize, String name, ECSNodeFlag status) {
        this.host = host;
        this.port = port;
        this.rangeStart = start;
        this.rangeEnd = end;
        this.serverLock = serverLock;
        this.writeLock = writeLock;
        this.cacheStrategy = cacheStrategy;
        this.cacheSize = cacheSize;
        this.name = name;
        this.status = status;
    }

    public ECSNode(String host, Integer port, String start, String end, Boolean serverLock, Boolean writeLock) {
        this.host = host;
        this.port = port;
        this.rangeStart = start;
        this.rangeEnd = end;
        this.serverLock = serverLock;
        this.writeLock = writeLock;
    }

    public ECSNode(String host, Integer port) {
        this.host = host;
        this.port = port;

        // default is it accepts everything
        rangeStart = "0".repeat(32);
        rangeEnd = "F".repeat(32);

        // default behaviour
        serverLock = true;
        writeLock = false;
    }

    public ECSNode() {
        // default is it accepts everything
        rangeStart = "0".repeat(32);
        rangeEnd = "F".repeat(32);

        // default behaviour
        serverLock = true;
        writeLock = false;
    }

    public String getCacheStrategy() {
        return cacheStrategy;
    }

    public void setCacheStrategy(String cacheStrategy) {
        this.cacheStrategy = cacheStrategy;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }

    /**
     * Returns whether the server is responsible for the request or not. Range isn't inclusive.
     */
    public boolean inRange(String hash) {
        // if startRange > endRange it means it wraps around and should be an or
        if (rangeStart.compareTo(rangeEnd) >= 0) {
            return hash.compareTo(rangeStart) >= 0 || hash.compareTo(rangeEnd) < 0;
        }

        return hash.compareTo(rangeStart) >= 0 && hash.compareTo(rangeEnd) < 0;
    }

    public boolean notResponsibleFor(String key) {
        return !inRange(Hasher.hash(key));
    }

    public boolean responsibleFor(String key) {
        return inRange(Hasher.hash(key));
    }

    public void updateRange(String start, String end) {
        if (start != null) {
            this.rangeStart = start;
        }
        if (end != null) {
            this.rangeEnd = end;
        }
    }

    public void updateMetadata(ECSNode ECSNode) {
        this.host = ECSNode.getHost();
        this.port = ECSNode.getPort();
        this.rangeStart = ECSNode.getRangeStart();
        this.rangeEnd = ECSNode.getRangeEnd();
        this.serverLock = ECSNode.getServerLock();
        this.writeLock = ECSNode.getWriteLock();
    }

    public boolean serverLocked() {
        return serverLock;
    }

    public boolean writeLocked() {
        return writeLock;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public Boolean getServerLock() {
        return serverLock;
    }

    public Boolean getWriteLock() {
        return writeLock;
    }

    public void setRangeStart(String rangeStart) {
        this.rangeStart = rangeStart;
    }

    public void setRangeEnd(String rangeEnd) {
        this.rangeEnd = rangeEnd;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public void setServerLock(Boolean serverLock) {
        this.serverLock = serverLock;
    }

    public void setWriteLock(Boolean writeLock) {
        this.writeLock = writeLock;
    }

    public String getRangeStart() {
        return rangeStart;
    }

    public String getRangeEnd() {
        return rangeEnd;
    }
}
