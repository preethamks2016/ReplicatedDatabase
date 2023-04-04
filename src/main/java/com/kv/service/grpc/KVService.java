package com.kv.service.grpc;

import com.kv.store.KVStore;
import com.kv.store.LogStore;
import com.kvs.Kvservice;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

public abstract class KVService {

//    protected KVSClient client;
    protected LogStore logStore;
    protected Logger logger;
    protected List<KVSClient> clients;

    protected KVStore kvStore;

    protected int commitIndex = -1;

    protected ScheduledExecutorService scheduledExecutor;

    protected ServiceType newServiceType;

    public KVService (LogStore logStore, List<Map<String, Object>> servers, KVStore kvStore) {
//        String serverAddress = "localhost:50051";
//        ManagedChannel channel = ManagedChannelBuilder.forTarget(serverAddress)
//                .usePlaintext()
//                .build();
//        this.client = new KVSClient(channel);
        clients = new ArrayList<KVSClient>();
        this.kvStore = kvStore;
        for (Map<String, Object> server : servers) {
            ManagedChannel channel = ManagedChannelBuilder.forTarget(server.get("ip").toString() + ":" + server.get("port").toString())
                                        .usePlaintext()
                                        .enableRetry()
                                        .maxRetryAttempts(10000)
                                        .build();

            KVSClient client = new KVSClient(channel);
            clients.add(client);
        }

        this.logStore = logStore;
        this.logger = LogManager.getLogger(KVService.class);
        BasicConfigurator.configure();
    }

    public abstract void put(int key, int value);
    public abstract int get(int key);
    public abstract ScheduledExecutorService start();

    public abstract void stop();

    public abstract ServiceType getType();

    public abstract Kvservice.APEResponse appendEntries(Kvservice.APERequest req);
}
