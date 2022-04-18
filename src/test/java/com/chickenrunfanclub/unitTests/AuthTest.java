package com.chickenrunfanclub.unitTests;

import com.chickenrunfanclub.TestUtils;
import com.chickenrunfanclub.app_kvAuth.AuthClient;
import com.chickenrunfanclub.app_kvAuth.AuthService;
import com.chickenrunfanclub.app_kvServer.KVServer;
import com.chickenrunfanclub.client.KVStore;
import com.chickenrunfanclub.shared.messages.IAuthMessage;
import org.junit.jupiter.api.*;

import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.*;


public class AuthTest {
    final static int port = 50002;
    static AuthService auth;
    private static final TestUtils utils = new TestUtils();

    @BeforeAll
    static void init() throws Exception {
        auth = new AuthService(port);
        auth.delete("A");
        auth.delete("B");
        auth.delete("C");
        auth.delete("D");
    }

    @Test
    public void testAuth() {
        Exception ex = null;
        try {
            // add
            assertEquals(IAuthMessage.StatusType.ADD_SUCCESS, auth.add("A", "aaa").getStatus());
            assertEquals(IAuthMessage.StatusType.ADD_SUCCESS, auth.add("B", "bbb").getStatus());
            assertEquals(IAuthMessage.StatusType.ADD_SUCCESS, auth.add("C", "ccc").getStatus());
            // update
            assertEquals(IAuthMessage.StatusType.PASSWORD_UPDATE, auth.add("A", "AAA").getStatus());
            // delete
            assertEquals(IAuthMessage.StatusType.DELETE_SUCCESS, auth.delete("B").getStatus());
            // authenticate
            assertEquals(IAuthMessage.StatusType.AUTH_ERROR, auth.authenticate("A", "aaa").getStatus());
            assertEquals(IAuthMessage.StatusType.AUTH_SUCCESS, auth.authenticate("A", "AAA").getStatus());
            assertEquals(IAuthMessage.StatusType.AUTH_ERROR, auth.authenticate("B", "bbb").getStatus());
            assertEquals(IAuthMessage.StatusType.AUTH_SUCCESS, auth.authenticate("C", "ccc").getStatus());
            assertEquals(IAuthMessage.StatusType.AUTH_ERROR, auth.authenticate("D", "ddd").getStatus());
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
    }

    @AfterAll
    static void clear() throws Exception {
        auth.delete("A");
        auth.delete("C");
    }

}

