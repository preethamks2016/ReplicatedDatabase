package com.kv.service.grpc;

import com.kv.store.KVStore;
import com.kv.store.LogStore;

import java.util.List;
import java.util.Map;

public class CandidateKVSService extends KVService{

    List<Map<String, Object>> servers;
    public CandidateKVSService(LogStore logStore,  List<Map<String, Object>> servers, KVStore kvStore) {
        super(logStore, servers, kvStore);
        this.servers = servers;
    }

    @Override
    public void put(int key, int value) {

    }

    @Override
    public int get(int key) {
        return 0;
    }

    @Override
    public void start() {

    }

    public com.kvs.Kvservice.APEResponse appendEntries(com.kvs.Kvservice.APERequest req) {
        // todo:
        return null;
    }

    @Override
    public ServiceType getType() {
        return ServiceType.CANDIDATE;
    }
}
