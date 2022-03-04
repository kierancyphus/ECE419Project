package com.chickenrunfanclub.ecs;

public interface IECSNode {

    /**
     * @return the name of the node (ie "Server 8.8.8.8")
     */
    public String getName();

    /**
     * @return the hostname of the node (ie "8.8.8.8")
     */
    public String getHost();

    /**
     * @return the port number of the node (ie 8080)
     */
    public int getPort();
}
