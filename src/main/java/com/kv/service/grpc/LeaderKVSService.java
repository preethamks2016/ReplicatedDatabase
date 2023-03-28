package com.kv.service.grpc;

import com.kv.store.LogStore;

public class LeaderKVSService extends KVService {

    LeaderKVSService(LogStore logStore) {
        super(logStore);
    }
    @Override
    public void put(int key, int value) {
        //get index
        //append entries to log
        //send appendEntries request
        //ack
        return;
    }

    @Override
    public void start() {

    }

    @Override
    public ServiceType getType() {
        return ServiceType.LEADER;
    }
}
