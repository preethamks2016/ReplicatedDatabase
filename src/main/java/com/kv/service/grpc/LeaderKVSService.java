package com.kv.service.grpc;

public class LeaderKVSService extends KVService {

    LeaderKVSService() {

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

}
