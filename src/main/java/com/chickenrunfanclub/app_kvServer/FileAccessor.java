package com.chickenrunfanclub.app_kvServer;

import com.chickenrunfanclub.shared.messages.IKVMessage;
import com.chickenrunfanclub.shared.messages.KVMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

public class FileAccessor {
    private String filename;
    private String key;
    private File file;
    private static Logger logger = LogManager.getLogger(FileAccessor.class);


    public FileAccessor(String filename, String key) {
        this.filename = filename;
        this.key = key;
        file = new File(filename);

    }

    synchronized public IKVMessage get() {
        try {
            Scanner scanner = new Scanner(file);
            String value = scanner.nextLine();
            return new KVMessage(key, value, IKVMessage.StatusType.GET_SUCCESS);
        } catch (FileNotFoundException e) {
            return new KVMessage(key, null, IKVMessage.StatusType.GET_ERROR);
        }
    }

    synchronized public IKVMessage put(String value) {
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

    public String getKey() {
        return key;
    }

    private static void writeToFile(String filename, String value) throws IOException {
        FileWriter writer = new FileWriter(filename);
        writer.write(value);
        writer.close();
    }
}
