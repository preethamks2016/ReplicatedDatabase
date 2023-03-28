package com.kv.service.grpc;

import com.kv.store.LogStore;

public class CandidateKVSService extends KVService{
    public CandidateKVSService(LogStore logStore) {
        super(logStore);
    }

    @Override
    public void put(int key, int value) {

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
