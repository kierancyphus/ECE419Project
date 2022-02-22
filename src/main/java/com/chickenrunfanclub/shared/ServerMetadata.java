package com.chickenrunfanclub.shared;

public class ServerMetadata {
    private String rangeStart;
    private String rangeEnd;
    private String host;
    private Integer port;
    private Boolean serverLock;
    private Boolean writeLock;

    public ServerMetadata(String host, Integer port, String start, String end, Boolean serverLock, Boolean writeLock) {
        this.host = host;
        this.port = port;
        this.rangeStart = start;
        this.rangeEnd = end;
        this.serverLock = serverLock;
        this.writeLock = writeLock;
    }

    public ServerMetadata(String host, Integer port) {
        this.host = host;
        this.port = port;

        // default is it accepts everything
        rangeStart = "0".repeat(32);
        rangeEnd = "F".repeat(32);

        // default behaviour
        serverLock = true;
        writeLock = false;
    }

    public ServerMetadata() {
        // default is it accepts everything
        rangeStart = "0".repeat(32);
        rangeEnd = "F".repeat(32);

        // default behaviour
        serverLock = true;
        writeLock = false;
    }

    /**
     * Returns whether the server is responsible for the request or not. Range isn't inclusive.
     * */
    public boolean inRange(String hash) {
        // if startRange > endRange it means it wraps around and should be an or
        if (rangeStart.compareTo(rangeEnd) > 0) {
            return hash.compareTo(rangeStart) >= 0 || hash.compareTo(rangeEnd) < 0;
        }

        return hash.compareTo(rangeStart) >= 0 && hash.compareTo(rangeEnd) < 0;
    }

    public boolean notResponsibleFor(String key) {
        return !inRange(Hasher.hash(key));
    }

    public void updateRange(String start, String end) {
        if (start != null) {
            this.rangeStart = start;
        }
        if (end != null) {
            this.rangeEnd = end;
        }
    }

    public void updateMetadata(ServerMetadata serverMetadata) {
        this.host = serverMetadata.getHost();
        this.port = serverMetadata.getPort();
        this.rangeStart = serverMetadata.getRangeStart();
        this.rangeEnd = serverMetadata.getRangeEnd();
        this.serverLock = serverMetadata.getServerLock();
        this.writeLock = serverMetadata.getWriteLock();
    }

    public boolean serverLocked() { return serverLock; }

    public boolean writeLocked() { return writeLock; }

    public String getHost() {
        return host;
    }

    public Integer getPort() {
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

    public void setWriteLock(Boolean writeLock) { this.writeLock = writeLock; }

    public String getRangeStart() {
        return rangeStart;
    }

    public String getRangeEnd() {
        return rangeEnd;
    }
}
