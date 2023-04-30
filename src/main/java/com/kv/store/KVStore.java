package com.kv.store;

import java.io.IOException;

public interface KVStore {
    int get(int key);

    void put(int key, int value);

    void stop() throws IOException;
}
