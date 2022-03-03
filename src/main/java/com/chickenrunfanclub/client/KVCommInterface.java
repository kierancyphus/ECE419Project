package com.chickenrunfanclub.client;

import com.chickenrunfanclub.shared.ServerMetadata;
import com.chickenrunfanclub.shared.messages.IKVMessage;

public interface KVCommInterface {

	/**
	 * Establishes a connection to the KV Server.
	 *
	 * @throws Exception
	 *             if connection could not be established.
	 */
	public void connect() throws Exception;

	/**
	 * disconnects the client from the currently connected server.
	 */
	public void disconnect();

	/**
	 * Inserts a key-value pair into the KVServer.
	 *
	 * @param key
	 *            the key that identifies the given value.
	 * @param value
	 *            the value that is indexed by the given key.
	 * @return a message that confirms the insertion of the tuple or an error.
	 * @throws Exception
	 *             if put command cannot be executed (e.g. not connected to any
	 *             KV server).
	 */
	public IKVMessage put(String key, String value) throws Exception;

	/**
	 * Retrieves the value for a given key from the KVServer.
	 *
	 * @param key
	 *            the key that identifies the value.
	 * @return the value, which is indexed by the given key.
	 * @throws Exception
	 *             if put command cannot be executed (e.g. not connected to any
	 *             KV server).
	 */
	public IKVMessage get(String key) throws Exception;

	public IKVMessage start() throws Exception;
	public IKVMessage stop() throws Exception;
	public IKVMessage shutDown() throws Exception;
	public IKVMessage lockWrite() throws Exception;
	public IKVMessage unlockWrite() throws Exception;
	public IKVMessage moveData(ServerMetadata metadata) throws Exception;
	public IKVMessage updateMetadata(ServerMetadata metadata) throws Exception;

}
