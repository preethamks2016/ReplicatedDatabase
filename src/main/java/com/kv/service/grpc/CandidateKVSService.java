package com.kv.service.grpc;

import com.kv.service.grpc.exception.LeaderElectedException;
import com.kv.store.LogStore;
import com.kvs.Kvservice;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;

public class CandidateKVSService extends KVService{

    List<Map<String, Object>> servers;

    Thread childThread;
    boolean valid;
    public CandidateKVSService(LogStore logStore,  List<Map<String, Object>> servers) {
        super(logStore, servers);
        this.servers = servers;
        valid = true;
    }

    @Override
    public void put(int key, int value) {

    }

    @Override
    public void start() throws LeaderElectedException {
        int votesReceived = 1;
        //(a) it wins the election, (b) another server establishes itself as leader, or (c) a period of time goes by with no winner
        childThread = new Thread(() -> {
            try {
                while (!Thread.interrupted()) {
                    // do some work
                }

            } catch (Exception e) {

            }
        });

        childThread.start();
        try {
            childThread.join();
        } catch (InterruptedException e) {
            // child thread was interrupted
           throw new LeaderElectedException();
        }

    }

    @Override
    public void stop() {
        valid = false;
    }

    public com.kvs.Kvservice.APEResponse appendEntries(com.kvs.Kvservice.APERequest req) {
        //need to fall back to follower state if received appendrpc from legitimate leader
        int currentTerm = 0 ; //todo:: update the current term of the follower

        if(req.getLeaderTerm() >= currentTerm) {
            // todo :: should we update the current term to leaders term ?
            childThread.interrupt();
        }


        // todo:
        return null;
    }

    @Override
    public ServiceType getType() {
        return ServiceType.CANDIDATE;
    }
}
