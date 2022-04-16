package com.chickenrunfanclub.app_kvAuth;

public interface IAuthService {

    /**
     * Check if user is in storage.
     * NOTE: does not modify any other properties
     *
     * @return true if user in storage, false otherwise
     */
    public boolean inStorage(String user);

    /**
     * Get the password associated with the key
     *
     * @return value associated with key
     * @throws Exception when key not in the key range of the server
     */
    public String getPassword(String user) throws Exception;

    /**
     * Put the user password pair into storage
     *
     * @throws Exception when key not in the key range of the server
     */
    public void addUserPassword(String user, String password) throws Exception;

    /**
     * Clear the storage of the server
     */
    public void clearStorage();

    /**
     * Starts running the server
     */
    public void run();

    /**
     * Abruptly stop the server without any additional actions
     * NOTE: this includes performing saving to storage
     */
    public void kill();

    /**
     * Gracefully stop the server, can perform any additional actions
     */
    public void close();

}

