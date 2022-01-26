package app_kvServer;

import org.apache.log4j.Logger;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class KVRepo {
    private String storePath;
    private static Logger logger = Logger.getRootLogger();
    private Set<String> keys;
    private int cacheSize;
    private IKVServer.CacheStrategy cacheStrategy;

    public KVRepo() {
        createStore();
    }

    public KVRepo(int cacheSize, IKVServer.CacheStrategy strategy) {
        createStore();
//        createCache();
        this.cacheSize = cacheSize;
        this.cacheStrategy = strategy;
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
        if (isNewKey) {
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
        String filename = "./store/" + key;
        File file = new File(filename);

        try {
            Scanner scanner = new Scanner(file);
            String value = scanner.nextLine();
            return new KVMessage(key, value, IKVMessage.StatusType.GET_SUCCESS);
        } catch (FileNotFoundException e) {
            return new KVMessage(key, null, IKVMessage.StatusType.GET_ERROR);
        }
    }

    public IKVMessage putDep(String key, String value) {
        // if we can't write to a file, then it means that it's in use

        String filename = this.storePath + key;
        File file = new File(filename);

        boolean isNewKey;
        try {
            isNewKey = file.createNewFile();
        } catch (Exception e) {
            logger.error("Error! Could not create entry for new key");
            return new KVMessage(key, value, IKVMessage.StatusType.PUT_ERROR);
        }

        // TODO: there is a race condition here - something might lock the file right after we created it
        if (isNewKey) {
            // TODO: this lock should loop
            lockFile(file);
            try {
                writeToFile(filename, value);
                unlockFile(file);
                return new KVMessage(key, value, IKVMessage.StatusType.PUT_SUCCESS);
            } catch (IOException e) {
                unlockFile(file);
                return new KVMessage(key, value, IKVMessage.StatusType.PUT_ERROR);
            }
        } else if (value != null) {
            // update

            // need to wait for file to become available
            while (!file.canWrite()) {
            }

            // TODO: have a check for if the file got deleted
            lockFile(file);
            try {
                writeToFile(filename, value);
                unlockFile(file);
                return new KVMessage(key, value, IKVMessage.StatusType.PUT_UPDATE);
            } catch (IOException e) {
                unlockFile(file);
                return new KVMessage(key, value, IKVMessage.StatusType.PUT_ERROR);
            }
        } else {
            // delete
            // TODO: change this because it will likely break something - Need to check what errors it throws
            boolean fileDeleteSuccess = file.delete();
            return new KVMessage(key, null, fileDeleteSuccess ? IKVMessage.StatusType.DELETE_SUCCESS : IKVMessage.StatusType.DELETE_ERROR);
        }
    }

    public IKVMessage getDep(String key) {
        String filename = "./store/" + key;
        File file = new File(filename);

        try {
            while (!file.canWrite()) {
            }
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

    private void createStore() {
        File store = new File("./store");
        if (!store.exists()) {
            boolean created = store.mkdirs();
            if (!created) {
                logger.error("Error! Could not create the Repo folder.");
            }
        }
        this.storePath = "./store/";
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

