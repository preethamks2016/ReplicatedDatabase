package com.kv.service.grpc;

import com.kv.store.Log;
import com.kv.store.LogStore;
import com.kvs.Kvservice;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class LeaderKVSService extends KVService {

    private ExecutorService executor;

    ReentrantLock lock;

    long lastSentTime = 0L;

    boolean valid = true;

    LeaderKVSService(LogStore logStore, List<Map<String, Object>> servers) {
        super(logStore, servers);
        lock = new ReentrantLock();
        executor = Executors.newFixedThreadPool(5);
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

            CompletionService<Kvservice.APEResponse> completionService = new ExecutorCompletionService<>(executor);
            for (KVSClient client : clients) {
                completionService.submit(() -> {
                    Kvservice.APEResponse response;
                    do {
                        response = client.appendEntries(request);
                        if (response.getSuccess()) break;
                    } while (true);

                    // todo : handle this request
                    return response;
                });
            }

            int ackCount = 1;
            int totalServers = clients.size() + 1;
            while (ackCount < totalServers) {
                try {
                    Future<Kvservice.APEResponse> completedTask = completionService.take();
                    if (completedTask.get().getSuccess()) {
                        ackCount++;
                    }
                    else {
                        // todo: handle failure
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    // handle exception from server
                }
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

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
        long threshold = 5 * 1000; // 5 seconds for now
        while (valid) {
            if (System.currentTimeMillis() - lastSentTime > threshold) {
                //send appendEntries
                Kvservice.APERequest request = Kvservice.APERequest.newBuilder()
                        .addAllEntry(null)
                        .build();
                CompletionService<Kvservice.APEResponse> completionService = new ExecutorCompletionService<>(executor);
                for (KVSClient client : clients) {
                    completionService.submit(() -> {
                        Kvservice.APEResponse response;
                        response = client.appendEntries(request);
                        return response;
                    });
                }
                lastSentTime = System.currentTimeMillis();
            }
        }

    }

    @Override
    public void stop() {
        // should be called when receiving append rpc entries with higher term
        valid = false;
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
