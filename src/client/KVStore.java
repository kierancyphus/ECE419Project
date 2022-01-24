package client;

import shared.messages.IKVMessage;
import shared.messages.KVMessage;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Objects;
import java.util.Scanner;
import java.util.stream.Stream;

public class KVStore implements KVCommInterface {
    String storePath;

    /**
     * Initialize KVStore with address and port of KVServer
     *
     * @param address the address of the KVServer
     * @param port    the port of the KVServer
     */
    public KVStore(String address, int port) {
        // TODO: this should be mainly using the communciation code from echo and injecting it
		// create the file store if it doesn't exist
		boolean storeCreated = createStore();

		if (!storeCreated) {
			// shit
		}

        this.storePath = "./store/";
    }

    @Override
    public void connect() throws Exception {
        // TODO Auto-generated method stub
    }

    @Override
    public void disconnect() {
        // TODO Auto-generated method stub
    }

    @Override
    public IKVMessage put(String key, String value) throws Exception {
        // if we can't write to a file, then it means that it's in use
		// TODO: figure out which exceptions to throw here since this is the library

        String filename = this.storePath + key;
        File file = new File(filename);

        if (file.createNewFile()) {
            lockFile(file);

            boolean writeSuccess = writeToFile(filename, value);
            if (!writeSuccess) {
                return new KVMessage(key, value, IKVMessage.StatusType.PUT_ERROR);
            }

            // change permissions so everyone can access now
            unlockFile(file);
            return new KVMessage(key, value, IKVMessage.StatusType.PUT_SUCCESS);

        } else if (value != null) {
            // update

            // need to wait for file to become available
            while (!file.canWrite()) {
            }

            // TODO: have a check for if the file got deleted
            lockFile(file);

            boolean writeSuccess = writeToFile(filename, value);
            if (!writeSuccess) {
                return new KVMessage(key, value, IKVMessage.StatusType.PUT_ERROR);
            }
            unlockFile(file);
            return new KVMessage(key, value, IKVMessage.StatusType.PUT_UPDATE);
        } else {
            // delete
            // TODO: change this because it will likely break something
            boolean fileDeleteSuccess = file.delete();
            return new KVMessage(key, null, fileDeleteSuccess ? IKVMessage.StatusType.DELETE_SUCCESS : IKVMessage.StatusType.DELETE_ERROR);
        }
    }

    @Override
    public IKVMessage get(String key) throws Exception {
		String filename = "./store/" + key;
		File file = new File(filename);

		if (file.exists()) {
			// wait until the file is free
			while (!file.canWrite()) {}
			Scanner scanner = new Scanner(file);
			String value = scanner.nextLine();
			return new KVMessage(key, value, IKVMessage.StatusType.GET_SUCCESS);

		} else {
			return new KVMessage(key, null, IKVMessage.StatusType.GET_ERROR);
		}
    }

    private static boolean writeToFile(String filename, String value) {
        try {
            FileWriter writer = new FileWriter(filename);
            writer.write(value);
            writer.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

	private boolean createStore() {
		File store = new File("./store");
		if (!store.exists()) {
			return store.mkdirs();
		}
		return true;
	}

    private boolean lockFile(File file) {
        return file.setWritable(false, false) && file.setWritable(true, true);
    }

    private boolean unlockFile(File file) {
        return file.setWritable(true, false);
    }

    public void nukeStore() {
        // TODO: make this return something more useful
        Stream.of(new File(this.storePath).listFiles()).forEach(File::delete);
    }

    public void listKeys() {
        Stream.of(Objects.requireNonNull(new File(this.storePath).listFiles())).forEach(file -> System.out.println(file.getName()));
        System.out.println("Done listing files");
    }
}
