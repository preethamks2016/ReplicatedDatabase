package com.kv.service.grpc;

import com.kv.store.LogStore;
import com.kvs.Kvservice;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public abstract class KVService {

//    protected KVSClient client;
    protected LogStore logStore;
    protected Logger logger;

    public KVService (LogStore logStore) {
//        String serverAddress = "localhost:50051";
//        ManagedChannel channel = ManagedChannelBuilder.forTarget(serverAddress)
//                .usePlaintext()
//                .build();
//        this.client = new KVSClient(channel);
        this.logStore = logStore;
        this.logger = LogManager.getLogger(KVService.class);
        BasicConfigurator.configure();
    }

    public abstract void put(int key, int value);
    public abstract void start();

    public abstract ServiceType getType();

    public abstract Kvservice.APEResponse appendEntries(Kvservice.APERequest req);
}
