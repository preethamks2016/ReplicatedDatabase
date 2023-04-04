package com.kv.service.grpc;

import com.kv.store.KVStore;
import com.kv.store.Log;
import com.kv.store.LogStore;
import com.kvs.Kvservice;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class LeaderKVSService extends KVService {

    private ExecutorService executor;

    // For every client maintain sync objects for every log index
    private List<ConcurrentHashMap<Integer, Object>> syncObjects;

    ReentrantLock lock;
    LeaderKVSService(LogStore logStore, List<Map<String, Object>> servers, KVStore kvStore) {
        super(logStore, servers, kvStore);
        lock = new ReentrantLock();
        executor = Executors.newFixedThreadPool(5);
        syncObjects = new ArrayList<ConcurrentHashMap<Integer, Object>>();
        for (int i = 0; i < clients.size(); i++) {
            syncObjects.add(new ConcurrentHashMap<Integer, Object>());
        }
    }

    public Kvservice.APEResponse appendEntries(KVSClient client, Log prevLog, Log currentLog) throws IOException {

        Kvservice.APERequest request =  populateAPERequest(prevLog, currentLog);
        Kvservice.APEResponse response = client.appendEntries(request);
        while (!response.getSuccess()) {
            if(response.getCurrentTerm() > request.getLeaderTerm()) {
                stop();
                //todo:: should stop execution??
                // follower term is greater than leader : fall back to follower state
            } else {
                // try comparing prev index - 1
                List<Kvservice.Entry> entries = new ArrayList<>();
                entries.add(getRequestEntry(prevLog));
                entries.addAll(request.getEntryList());
                if (prevLog.getIndex() != 0) {
                    Optional<Log> optionalLog = logStore.ReadAtIndex(prevLog.getIndex() - 1);
                    prevLog = optionalLog.get();
                }
                else {
                    prevLog = null;
                }

                request = setPrevLogEntries(request, prevLog, entries);
                response = client.appendEntries(request);
            }
        }
        return response;
    }

    private Kvservice.APERequest setPrevLogEntries(Kvservice.APERequest request, Log prevLog, List<Kvservice.Entry> entries) {

        Kvservice.APERequest newRequest = Kvservice.APERequest.newBuilder()
                .setLeaderTerm(request.getLeaderTerm())
                .setPrevLogIndex(prevLog == null ? -1 : prevLog.getIndex())
                .setPrevLogTerm(prevLog == null ? -1 : prevLog.getTerm())
                .addAllEntry(entries)
                .setLeaderCommitIdx(commitIndex)
                .build();
        return newRequest;
    }

    private Kvservice.Entry getRequestEntry(Log log) {
        Kvservice.Entry entry = Kvservice.Entry.newBuilder()
                .setIndex(log.getIndex())
                .setTerm(log.getTerm())
                .setKey(log.getKey())
                .setValue(log.getValue()).build();
        return entry;
    }

    @Override
    public void put(int key, int value) {
        try {
            lock.lock();
            Log prevLog;
            Log currentLog;
            Optional<Log> optionalLog = logStore.getLastLogEntry();

            // write to own log
            if (optionalLog.isPresent()) {
                prevLog = optionalLog.get();
                currentLog = new Log(prevLog.getIndex() + 1, prevLog.getTerm(), key, value);
                logStore.WriteToIndex(currentLog, currentLog.getIndex());
            } else {
                prevLog = null;
                currentLog =  new Log(0, 0, key, value);
                logStore.WriteToIndex(currentLog, currentLog.getIndex());
            }

            // create new sync object for current index
            for (int i = 0; i < clients.size(); i++)
                syncObjects.get(i).put(currentLog.getIndex(), new Object());

            lock.unlock();

            Kvservice.APERequest request =  populateAPERequest(prevLog, currentLog);

            int currentLogIndex = currentLog.getIndex();
            CompletionService<Kvservice.APEResponse> completionService = new ExecutorCompletionService<>(executor);
            for (int i = 0; i < clients.size(); i++) {
                int clientIdx = i;
                completionService.submit(() -> {

                    // synchronization wait for previous log index to complete
                    if (syncObjects.get(clientIdx).contains(currentLogIndex-1)) {
                        Object prevObject = syncObjects.get(clientIdx).get(currentLogIndex-1);

                        try {
                            synchronized (prevObject) {
                                prevObject.wait();
                            }
                        }
                        catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    Kvservice.APEResponse response = appendEntries(clients.get(clientIdx), prevLog, currentLog);
                    // notify future put requests
                    Object currentSyncObject = syncObjects.get(clientIdx).get(currentLogIndex);
                    synchronized (currentSyncObject) {
                        syncObjects.get(clientIdx).remove(currentLogIndex);
                        currentSyncObject.notify();
                    }
                    return response;
                });
            }

            int ackCount = 1;
            int totalServers = clients.size() + 1;
            while (ackCount <= totalServers/2) {
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
            synchronized (this) {
                for(int index = commitIndex + 1; index <= currentLogIndex; index++){
                    Optional<Log> log = logStore.ReadAtIndex(index);
                    kvStore.put(log.get().getKey(), log.get().getValue());
                }
                commitIndex = currentLogIndex;
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return;
    }

    @Override
    public int get(int key) {
        return this.kvStore.get(key);
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
                .setLeaderCommitIdx(commitIndex)
                .build();
        return request;

    }

    @Override
    public ScheduledExecutorService start() {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(() -> {
            Kvservice.APERequest request = Kvservice.APERequest.newBuilder()
                    .addAllEntry(null)
                    .build();
            //todo :: may be add all required details
            CompletionService<Kvservice.APEResponse> completionService = new ExecutorCompletionService<>(executor);
            for (KVSClient client : clients) {
                completionService.submit(() -> {
                    Kvservice.APEResponse response;
                    response = client.appendEntries(request);
                    return response;
                });
            }
        }, 0, 5, TimeUnit.SECONDS);
        scheduledExecutor = executor;
        return executor;
    }

    @Override
    public void stop() {
        System.out.println("Stop called");
        newServiceType = ServiceType.FOLLOWER;
        scheduledExecutor.shutdownNow();
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
