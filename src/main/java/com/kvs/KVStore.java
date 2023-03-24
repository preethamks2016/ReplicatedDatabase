package com.kvs;

public interface KVStore {
    int get(int key);

    void put(int key, int value);
}
