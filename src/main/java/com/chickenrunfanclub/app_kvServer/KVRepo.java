package com.chickenrunfanclub.app_kvServer;

import com.chickenrunfanclub.shared.Hasher;
import com.chickenrunfanclub.shared.ServerMetadata;
import com.chickenrunfanclub.shared.messages.IKVMessage;
import com.chickenrunfanclub.shared.messages.KVMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class KVRepo {
    // Essentials
    private String storePath;
    private static Logger logger = LogManager.getLogger(KVRepo.class);
    private Set<String> keys;
    private String defaultStorePath = "./store";
    private ConcurrentHashMap<String, FileAccessor> filesInUse;

    // Cache
    private int cacheSize;
    private IKVServer.CacheStrategy cacheStrategy;

    // Server Metadata
    private ServerMetadata serverMetadata;
    private boolean writeLock = false;
    private boolean repoLocked = true;

    // Consistent Hashing
    private HashMap<String, String> hashes;

    public KVRepo() {
        createStore(defaultStorePath);
        initializeHash();
    }

    public KVRepo(int cacheSize, IKVServer.CacheStrategy strategy) {
        createStore(defaultStorePath);
//        createCache();
        this.cacheSize = cacheSize;
        this.cacheStrategy = strategy;

        initializeHash();
    }

    public KVRepo(int cacheSize, IKVServer.CacheStrategy strategy, String storePath, ServerMetadata serverMetadata) {
        createStore(storePath);
        this.serverMetadata = serverMetadata;
        initializeHash();
    }

    public IKVMessage put(String key, String value) {
        if (serverMetadata.notResponsibleFor(key)) {
            logger.info("Repo not responsible. Put<" + key +", " + value + "> failed");
            return new KVMessage(key, value, IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
        }

        if (serverMetadata.serverLocked()) {
            logger.info("Repo is locked. Put<" + key +", " + value + "> failed");
            return new KVMessage(key, value, IKVMessage.StatusType.SERVER_STOPPED);
        }

        if (serverMetadata.writeLocked()) {
            logger.info("Repo is write locked. Put<" + key +", " + value + "> failed");
            return new KVMessage(key, value, IKVMessage.StatusType.SERVER_WRITE_LOCK);
        }

        String filename = this.storePath + key;
        IKVMessage response = null;
        FileAccessor newFileAccessor = new FileAccessor(filename, key);
        // check if we are the first file to use this
        if (filesInUse.putIfAbsent(key, newFileAccessor) == null) {
            FileAccessor fa = filesInUse.get(key);
            response = fa.put(value);
            filesInUse.remove(key);
        }
        // if another file is using it, we need to wait until they are done and have removed the entry
        while (filesInUse.putIfAbsent(key, newFileAccessor) != null){
            FileAccessor fa = filesInUse.get(key);
            response = fa.put(value);
            filesInUse.remove(key);
        }
        return response;
    }

    public IKVMessage get(String key) {
        if (serverMetadata.notResponsibleFor(key)) {
            return new KVMessage(key, null, IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
        }

        if (serverMetadata.serverLocked()) {
            return new KVMessage(key, null, IKVMessage.StatusType.SERVER_STOPPED);
        }

        String filename = storePath + key;
        // Need to continuously poll until we are the one in control of the file
        IKVMessage response = null;
        FileAccessor newFileAccessor = new FileAccessor(filename, key);
        if (filesInUse.putIfAbsent(key, newFileAccessor) == null) {
            FileAccessor fa = filesInUse.get(key);
            response = fa.get();
            filesInUse.remove(key);
        }
        while (filesInUse.putIfAbsent(key, newFileAccessor) != null){
            FileAccessor fa = filesInUse.get(key);
            response = fa.get();
            filesInUse.remove(key);
        }
        return response;
    }

    private void createStore(String filepath) {
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

    public List<Map.Entry<String, String>> getEntriesInHashRange(ServerMetadata rangeServerMetadata) {
        // update the hashes to make sure it is up to date (I could do this incrementally but this works)
        // I could also do this for each put, but I'd rather not incur the overhead on the user
        initializeHash();

        return hashes.entrySet()
                .stream()
                .filter(entry -> rangeServerMetadata.inRange(entry.getKey()))  // filter out those not in range
                .map(entry -> new AbstractMap.SimpleEntry<>(entry.getValue(), get(entry.getValue()).getValue()))  // construct kv pairs
                .collect(Collectors.toList());
    }

    private void initializeHash() {
        // this stores all the files names and hashes in memory which could take quite a lot of time on start up
        hashes = new HashMap<>();
        File storePathFile = new File(this.storePath);

        for (File file: Objects.requireNonNull(storePathFile.listFiles())) {
            String hash = Hasher.hash(file.getName());
            if (serverMetadata.inRange(hash)) {
                hashes.put(hash, file.getName());
            }
        }
    }
}
