package com.kv.service.grpc;

import com.kv.store.KVStore;
import com.kv.store.LogStore;
import com.kvs.Kvservice;
import io.grpc.Metadata;
import io.grpc.Status;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

public class CandidateKVSService extends KVService{

    List<Map<String, Object>> servers;
    public CandidateKVSService(LogStore logStore,  List<Map<String, Object>> servers, KVStore kvStore) {
        super(logStore, servers, kvStore);
        this.servers = servers;
    }

    @Override
    public void put(int key, int value) {
        ThrowExceptionToRejectGetPut();
    }

    @Override
    public int get(int key) {
        ThrowExceptionToRejectGetPut();
        return 0;
    }

    @Override
    public ScheduledExecutorService start() {
        return null;
    }

    @Override
    public void stop(ServiceType newType) {

    }

    public com.kvs.Kvservice.APEResponse appendEntries(com.kvs.Kvservice.APERequest req) {
        try {
            int currentTerm = logStore.getCurrentTerm();
            if (req.getLeaderTerm() >= currentTerm) {
                // update term
                logStore.setTerm(req.getLeaderTerm());
                stop(ServiceType.FOLLOWER); // return to follower state
                throw new Exception("Make the RPC call fail");
            }
            else {
                return Kvservice.APEResponse.newBuilder().setCurrentTerm(currentTerm).setSuccess(false).build();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Kvservice.RVResponse requestVotes(Kvservice.RVRequest req) {
        return null;
    }

    @Override
    public ServiceType getType() {
        return ServiceType.CANDIDATE;
    }
}
