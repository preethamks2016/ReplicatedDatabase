package com.kv.store;

public interface KVStore {
    int get(int key);

    void put(int key, int value);
}
