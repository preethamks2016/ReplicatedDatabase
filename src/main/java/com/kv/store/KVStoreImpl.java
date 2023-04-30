package com.kv.store;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import java.io.File;
import java.io.IOException;

import static org.iq80.leveldb.impl.Iq80DBFactory.*;

public class KVStoreImpl implements KVStore {
    DB db;

    public KVStoreImpl(int port) throws IOException {
        Options options = new Options();
        options.createIfMissing(true);
        db = factory.open(new File("levelDBStore" + port), options);
    }

    @Override
    public int get(int key) {
        return Integer.parseInt(asString(db.get(bytes(String.valueOf(key)))));
    }

    @Override
    public void put(int key, int value) {
        db.put(bytes(String.valueOf(key)), bytes(String.valueOf(value)));
    }

    @Override
    public void stop() throws IOException {
        db.close();
    }
}
