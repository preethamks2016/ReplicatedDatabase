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

    @Override
    public ServiceType getType() {
        return ServiceType.CANDIDATE;
    }
}
