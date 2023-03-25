package com.kv.store;

import com.kv.store.KVStore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KVStoreImpl implements KVStore {
    Map<Integer, Integer> map;

    public KVStoreImpl() {
        this.map = new ConcurrentHashMap<>();
    }

    @Override
    public int get(int key) {
        return map.get(key);
    }

    @Override
    public void put(int key, int value) {
        map.put(key, value);
    }
}
