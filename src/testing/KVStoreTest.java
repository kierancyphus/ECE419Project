package testing;

import client.KVStore;
import junit.framework.TestCase;
import org.junit.Test;
import shared.messages.IKVMessage;

public class KVStoreTest extends TestCase {
    private KVStore store;

    public void setUp() {
        store = new KVStore(null, 3000);
        store.nukeStore();
    }

    @Test
    public void testCreateFileWithPut() {
        String key = "key";
        String value = "value";
        IKVMessage response = null;
        Exception ex = null;


        try {
            response = store.put(key, value);
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == IKVMessage.StatusType.PUT_SUCCESS);
    }

    @Test
    public void testUpdateFileWithPut() {
        String key = "key";
        String value = "value";
        IKVMessage response = null;
        Exception ex = null;

        try {
            response = store.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == IKVMessage.StatusType.PUT_SUCCESS);

        try {
            response = store.put(key, "Some other value");
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == IKVMessage.StatusType.PUT_UPDATE);

    }
}
