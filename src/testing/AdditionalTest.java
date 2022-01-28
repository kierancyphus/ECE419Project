package testing;

import client.KVStore;
import org.junit.Test;

import junit.framework.TestCase;
import shared.messages.IKVMessage;

public class AdditionalTest extends TestCase {
	
	// TODO add your test cases, at least 3
	
	@Test
	public void testPersistence() {
		// put and disconnect, then reconnect and get
		String key = "hello";
		String value = "world";
		IKVMessage putResponse = null, getResponse = null;
		Exception ex = null;

		KVStore kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}
		// delete the key value pair in case already exist
		try {
			kvClient.put(key,null);
		} catch (Exception e) {
		}
		// put
		try {
			putResponse = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}
		// disconnect
		kvClient.disconnect();
		kvClient = null;
		// get
		kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
			getResponse = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && putResponse.getStatus() == IKVMessage.StatusType.PUT_SUCCESS
				&& getResponse.getValue().equals(value));
	}


	@Test
	public void testTwoUserPutGet() {
		// two users, one user put the other update
		String key = "hello";
		String value = "world", newValue = "hello!";
		IKVMessage putResponse = null, updateResponse = null, getResponse = null;
		Exception ex = null;

		KVStore kvClient1 = new KVStore("localhost", 50000);
		KVStore kvClient2 = new KVStore("localhost", 50000);
		try {
			kvClient1.connect();
			kvClient2.connect();
		} catch (Exception e) {
			ex = e;
		}
		// delete the key value pair in case already exist
		try {
			kvClient1.put(key,null);
		} catch (Exception e) {
		}

		// user 1 put
		try {
			putResponse = kvClient1.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		// user 2 updates it
		try {
			updateResponse = kvClient2.put(key, newValue);
		} catch (Exception e) {
			ex = e;
		}

		// user 1 get
		try {
			getResponse = kvClient1.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && putResponse.getStatus() == IKVMessage.StatusType.PUT_SUCCESS
				&& updateResponse.getStatus() == IKVMessage.StatusType.PUT_UPDATE
				&& getResponse.getValue().equals(newValue));
	}

	@Test
	public void testCapacity() {
		// put  1000 pairs, then get them
		IKVMessage putResponse = null, getResponse = null;
		Exception ex = null;

		KVStore kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}

		// put 500 pairs
		try {
			for (int i = 0; i < 1000; i++) {
				kvClient.put(String.valueOf(i), String.valueOf(i));
			}
		} catch (Exception e) {
			ex = e;
		}

		// get
		try {
			for (int i = 0; i < 1000; i++) {
				getResponse = kvClient.get(String.valueOf(i));
				assertTrue(ex == null && getResponse.getValue().equals(String.valueOf(i)));
			}
		} catch (Exception e) {
			ex = e;
		}
	}

}
