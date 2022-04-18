package com.chickenrunfanclub.app_kvAuth;

import com.chickenrunfanclub.shared.messages.AuthMessage;
import com.chickenrunfanclub.shared.messages.IAuthMessage;
import com.chickenrunfanclub.shared.messages.IKVMessage;
import com.chickenrunfanclub.shared.messages.KVMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

public class AuthFileAccessor {
    private String filename;
    private String key;
    private File file;
    private static Logger logger = LogManager.getLogger(AuthFileAccessor.class);


    public AuthFileAccessor(String filename, String key) {
        this.filename = filename;
        this.key = key;
        file = new File(filename);
    }

    synchronized public IAuthMessage get() {
        try {
            Scanner scanner = new Scanner(file);
            String value = scanner.nextLine();
            return new AuthMessage(key, value, IAuthMessage.StatusType.GET_SUCCESS);
        } catch (FileNotFoundException e) {
            return new AuthMessage(key, null, IAuthMessage.StatusType.GET_ERROR);
        }
    }

    synchronized public IAuthMessage put(String value) {
        boolean isNewKey;
        try {
            isNewKey = file.createNewFile();
        } catch (Exception e) {
            logger.error("Error! Could not create entry for new key");
            return new AuthMessage(key, value, IAuthMessage.StatusType.ADD_ERROR);
        }
        if (isNewKey && value != null) {
            // create
            try {
                writeToFile(filename, value);
                return new AuthMessage(key, value, IAuthMessage.StatusType.ADD_SUCCESS);
            } catch (IOException e) {
                return new AuthMessage(key, value, IAuthMessage.StatusType.ADD_ERROR);
            }
        } else if (value != null) {
            // update
            try {
                writeToFile(filename, value);
                return new AuthMessage(key, value, IAuthMessage.StatusType.PASSWORD_UPDATE);
            } catch (IOException e) {
                return new AuthMessage(key, value, IAuthMessage.StatusType.ADD_ERROR);
            }
        } else {
            // delete
            boolean fileDeleteSuccess = file.delete();
            return new AuthMessage(key, null, fileDeleteSuccess ? IAuthMessage.StatusType.DELETE_SUCCESS : IAuthMessage.StatusType.DELETE_ERROR);
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
