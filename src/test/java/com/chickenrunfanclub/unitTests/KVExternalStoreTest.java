package com.chickenrunfanclub.unitTests;

import com.chickenrunfanclub.app_kvAuth.AuthClient;
import com.chickenrunfanclub.app_kvECS.AllServerMetadata;
import com.chickenrunfanclub.app_kvECS.ECSClient;
import com.chickenrunfanclub.client.KVExternalStore;
import com.chickenrunfanclub.shared.messages.IAuthMessage;
import com.chickenrunfanclub.shared.messages.IKVMessage;
import com.chickenrunfanclub.shared.messages.KVMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class KVExternalStoreTest {

    @Test
    public void helper() {
        KVExternalStore store = new KVExternalStore("./apiGateway.cfg");

        store.setUsername("test");
        store.setPassword("eubhadfbhjfd");

        IKVMessage response = store.put("u", "lovely person", 0);

        System.out.println(response);

        assertFalse(true);
    }

    @Test
    public void authHelper() {
        AuthClient client = new AuthClient();
        String username = "test";
        String password = "eubhadfbhjfd";

        try {
            IAuthMessage message = client.authenticate(username, password);
            System.out.println(message);
        } catch (Exception e) {

        }
    }
    // Add some stuff when servers are up and working
}
