package com.chickenrunfanclub.app_kvServer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.chickenrunfanclub.shared.messages.IKVMessage;
import com.chickenrunfanclub.shared.messages.KVMessage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class KVRepo {
    private String storePath;
    private static Logger logger = LogManager.getLogger(KVRepo.class);
    private Set<String> keys;
    private int cacheSize;
    private IKVServer.CacheStrategy cacheStrategy;
    private String defaultStorePath = "./store";

    public KVRepo() {
        createStore(defaultStorePath);
    }

    public KVRepo(int cacheSize, IKVServer.CacheStrategy strategy) {
        createStore(defaultStorePath);
//        createCache();
        this.cacheSize = cacheSize;
        this.cacheStrategy = strategy;
    }

    public KVRepo(int cacheSize, IKVServer.CacheStrategy strategy, String storePath) {
        createStore(storePath);
    }

    synchronized public IKVMessage put(String key, String value) {
        String filename = this.storePath + key;
        File file = new File(filename);

        boolean isNewKey;
        try {
            isNewKey = file.createNewFile();
        } catch (Exception e) {
            logger.error("Error! Could not create entry for new key");
            return new KVMessage(key, value, IKVMessage.StatusType.PUT_ERROR);
        }
        if (isNewKey && value != null) {
            // create
            try {
                writeToFile(filename, value);
                return new KVMessage(key, value, IKVMessage.StatusType.PUT_SUCCESS);
            } catch (IOException e) {
                return new KVMessage(key, value, IKVMessage.StatusType.PUT_ERROR);
            }
        } else if (value != null) {
            // update
            try {
                writeToFile(filename, value);
                return new KVMessage(key, value, IKVMessage.StatusType.PUT_UPDATE);
            } catch (IOException e) {
                return new KVMessage(key, value, IKVMessage.StatusType.PUT_ERROR);
            }
        } else {
            // delete
            boolean fileDeleteSuccess = file.delete();
            return new KVMessage(key, null, fileDeleteSuccess ? IKVMessage.StatusType.DELETE_SUCCESS : IKVMessage.StatusType.DELETE_ERROR);
        }
    }

    synchronized public IKVMessage get(String key) {
        String filename = this.storePath + key;
        File file = new File(filename);

        try {
            Scanner scanner = new Scanner(file);
            String value = scanner.nextLine();
            return new KVMessage(key, value, IKVMessage.StatusType.GET_SUCCESS);
        } catch (FileNotFoundException e) {
            return new KVMessage(key, null, IKVMessage.StatusType.GET_ERROR);
        }
    }

    private static void writeToFile(String filename, String value) throws IOException {
        FileWriter writer = new FileWriter(filename);
        writer.write(value);
        writer.close();
    }

    private void createStore(String filepath) {
        File store = new File(filepath);
        if (!store.exists()) {
            boolean created = store.mkdirs();
            if (!created) {
                logger.error("Error! Could not create the Repo folder.");
            }
        }
        this.storePath = filepath + "/";
    }

    private boolean lockFile(File file) {
        return file.setWritable(false, false) && file.setWritable(true, true);
    }

    private boolean unlockFile(File file) {
        return file.setWritable(true, false);
    }

    public boolean inStorage(String key) {
        String filename = this.storePath + key;
        File file = new File(filename);
        return file.exists();
    }

    public void nukeStore() {
        // TODO: make this return something more useful
        File file = new File(this.storePath);
        for (File otherFile: file.listFiles()) {
            otherFile.delete();
        }
    }

    public Set<String> listKeys() {
        File file = new File(this.storePath);
        return Arrays.stream(Objects.requireNonNull(file.listFiles())).map(File::getName).collect(Collectors.toSet());

//        for (File otherFile: file.listFiles()) {
//            System.out.println(otherFile.getName());
//        }
//        System.out.println("Done listing files");
    }
}

