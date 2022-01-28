package testing;

import client.KVStore;
import org.junit.Test;

import junit.framework.TestCase;
import shared.messages.IKVMessage;

public class AdditionalTest extends TestCase {
	
	// TODO add your test cases, at least 3
	
	@Test
	public void testPersistence() {
		String key = "foo2";
		String value = "bar2";
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
	public void testString() {
		String test = "something" + null + " else";
		System.out.println(test);
	}

}
