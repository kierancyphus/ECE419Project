package com.chickenrunfanclub.app_kvAuth;


import com.chickenrunfanclub.shared.messages.IAuthMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class AuthRepo {
    // Essentials
    private String storePath;
    private static Logger logger = LogManager.getLogger(AuthRepo.class);
    private Set<String> keys;
    private String defaultStorePath = ".../auth";
    private ConcurrentHashMap<String, AuthFileAccessor> filesInUse;

    public AuthRepo() {
        createAuthStore(defaultStorePath);
    }

    public IAuthMessage put(String key, String value) {

        String filename = this.storePath + key;
        IAuthMessage response = null;
        AuthFileAccessor newAuthFileAccessor = new AuthFileAccessor(filename, key);
        // check if we are the first file to use this
        if (filesInUse.putIfAbsent(key, newAuthFileAccessor) == null) {
            AuthFileAccessor fa = filesInUse.get(key);
            response = fa.put(value);
            filesInUse.remove(key);
        }
        // if another file is using it, we need to wait until they are done and have removed the entry
        while (filesInUse.putIfAbsent(key, newAuthFileAccessor) != null) {
            AuthFileAccessor fa = filesInUse.get(key);
            response = fa.put(value);
            filesInUse.remove(key);
        }
        return response;
    }

    public IAuthMessage get(String key) {
        String filename = storePath + key;
        // Need to continuously poll until we are the one in control of the file
        IAuthMessage response = null;
        AuthFileAccessor newFileAccessor = new AuthFileAccessor(filename, key);
        if (filesInUse.putIfAbsent(key, newFileAccessor) == null) {
            AuthFileAccessor fa = filesInUse.get(key);
            response = fa.get();
            filesInUse.remove(key);
        }
        while (filesInUse.putIfAbsent(key, newFileAccessor) != null) {
            AuthFileAccessor fa = filesInUse.get(key);
            response = fa.get();
            filesInUse.remove(key);
        }
        return response;
    }

    private void createAuthStore(String filepath) {
        File store = new File(filepath);
        if (!store.exists()) {
            boolean created = store.mkdirs();
            if (!created) {
                logger.error("Error! Could not create the Repo folder.");
            }
        }
        storePath = filepath + "/";
        filesInUse = new ConcurrentHashMap<>();
    }

    public boolean inStorage(String key) {
        String filename = storePath + key;
        File file = new File(filename);
        return file.exists();
    }

    public void nukeStore() {
        // TODO: make this return something more useful
        File file = new File(this.storePath);
        for (File otherFile : file.listFiles()) {
            otherFile.delete();
        }
    }

    public Set<String> listKeys() {
        File file = new File(this.storePath);
        return Arrays.stream(Objects.requireNonNull(file.listFiles())).map(File::getName).collect(Collectors.toSet());
    }
}
