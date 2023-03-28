package com.kv.service.grpc;

import com.kv.store.LogStore;
import com.kvs.Kvservice;

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

    @Override
    public Kvservice.APEResponse appendEntries(Kvservice.APERequest req) {
        logger.error("Invalid call: Leader cannot receive append entries");
        //todo: throw exception
        return null;
    }
}
