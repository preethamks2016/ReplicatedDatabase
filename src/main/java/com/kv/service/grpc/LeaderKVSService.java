package com.kv.service.grpc;

import com.kv.store.Log;
import com.kv.store.LogStore;
import com.kvs.Kvservice;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

public class LeaderKVSService extends KVService {

    private ExecutorService executor;

    ReentrantLock lock;
    LeaderKVSService(LogStore logStore) {
        super(logStore);
        lock = new ReentrantLock();
    }

    @Override
    public void put(int key, int value) {
        try {
            lock.lock();
            Log prevLog;
            Log currentLog;
            Optional<Log> optionalLog = logStore.getLastLogEntry();
            if (optionalLog.isPresent()) {
                prevLog = optionalLog.get();
                currentLog = new Log(prevLog.getIndex() + 1, prevLog.getTerm(), key, value);
                logStore.WriteToIndex(currentLog, currentLog.getIndex());
            } else {
                prevLog = null;
                currentLog =  new Log(0, 0, key, value);
                logStore.WriteToIndex(currentLog, currentLog.getIndex());
            }
            lock.unlock();

            Kvservice.APERequest request =  populateAPERequest(prevLog, currentLog);
            //TODO :: send APE request

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //get index
        //append entries to log
        //send appendEntries request
        //ack
        return;
    }

    private Kvservice.APERequest populateAPERequest(Log prevLog, Log currentLog) {

        List<Kvservice.Entry> entries = new ArrayList<>();
        Kvservice.Entry entry = Kvservice.Entry.newBuilder()
                .setIndex(currentLog.getIndex())
                .setTerm(currentLog.getTerm())
                .setKey(currentLog.getKey())
                .setValue(currentLog.getValue()).build();
        entries.add(entry);
        Kvservice.APERequest request = Kvservice.APERequest.newBuilder()
                .setLeaderTerm(currentLog.getTerm())
                .setPrevLogIndex(prevLog == null ? -1 : prevLog.getIndex())
                .setPrevLogTerm(prevLog == null ? -1 : prevLog.getTerm())
                .addAllEntry(entries)
                .build();
        return request;

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
