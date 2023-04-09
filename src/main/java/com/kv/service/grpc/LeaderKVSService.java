package com.kv.service.grpc;

import com.kv.service.grpc.exception.NoLongerLeaderException;
import com.kv.store.KVStore;
import com.kv.store.Log;
import com.kv.store.LogStore;
import com.kvs.Kvservice;
import io.grpc.Metadata;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class LeaderKVSService extends KVService {

    private ExecutorService executor;

    // For every client maintain sync objects for every log index
    private List<ConcurrentHashMap<Integer, Object>> syncObjects;

    private List<ConcurrentHashMap<Integer, Integer>> resultValidation;
    private Map<String, String> portToIP;

    ReentrantLock lock;
    LeaderKVSService(LogStore logStore, List<Map<String, Object>> servers, KVStore kvStore, int port) {
        super(logStore, servers, kvStore, port);
        lock = new ReentrantLock();
        executor = Executors.newFixedThreadPool(5);
        syncObjects = new ArrayList<ConcurrentHashMap<Integer, Object>>();
        resultValidation = new ArrayList<ConcurrentHashMap<Integer, Integer>>();
        for (int i = 0; i < clients.size(); i++) {
            syncObjects.add(new ConcurrentHashMap<Integer, Object>());
            resultValidation.add(new ConcurrentHashMap<Integer, Integer>());
        }

        portToIP = new HashMap<String, String>();
        for (Map<String, Object> server : servers) {
            portToIP.put(server.get("port").toString(), server.get("ip").toString());
        }
    }

    public Kvservice.APEResponse appendEntries(KVSClient client, Log prevLog, Log currentLog) throws IOException, NoLongerLeaderException {

        Kvservice.APERequest request =  populateAPERequest(prevLog, currentLog);
        Kvservice.APEResponse response = client.appendEntries(request);
        while (!response.getSuccess()) {
            if(response.getCurrentTerm() > request.getLeaderTerm()) {
                stop(ServiceType.FOLLOWER);
                throw new NoLongerLeaderException(0);
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
                .setLeaderCommitIdx(logStore.getCommitIndex())
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

    public class MyRunnable implements Callable<Kvservice.APEResponse> {

        private final int clientIdx;
        private final int currentLogIndex;

        private final Log prevLog;

        private final Log currentLog;


        public MyRunnable( int clientIndex, int currentLogIndex, Log prevLog, Log currentLog) {
            this.clientIdx = clientIndex;
            this.currentLogIndex = currentLogIndex;
            this.prevLog = prevLog;
            this.currentLog = currentLog;
        }

        @SneakyThrows
        @Override
        public Kvservice.APEResponse call() {
            // Do something with the arguments...
            try {
                synchronized (syncObjects.get(clientIdx).get(currentLogIndex - 1)) {
                    Object prevObject = syncObjects.get(clientIdx).get(currentLogIndex - 1);


                    //synchronized (prevObject) {
                        while (resultValidation.get(clientIdx).get(currentLogIndex - 1) == 0) {
                            prevObject.wait();
                        }
                    }
                    
                syncObjects.get(clientIdx).remove(currentLogIndex - 1);

            } catch (InterruptedException e) {
                    e.printStackTrace();
            } catch (NullPointerException e) {
                //it means that object was not present so no need to wait
                System.out.println("NULL POINTER");
            }

            Kvservice.APEResponse response = null;
            try {
                response = appendEntries(clients.get(clientIdx), prevLog, currentLog);
            } catch (NoLongerLeaderException e) {
                executor.shutdownNow();
            } catch (Exception e) {
                e.printStackTrace();
            }
            // notify future put requests
            Object currentSyncObject = syncObjects.get(clientIdx).get(currentLogIndex);
            synchronized (currentSyncObject) {
                resultValidation.get(clientIdx).put(currentLogIndex, 1);

                //syncObjects.get(clientIdx).remove(currentLogIndex);
                currentSyncObject.notify();
            }
            return response;
        }
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
                currentLog = new Log(prevLog.getIndex() + 1, logStore.getCurrentTerm(), key, value);
                logStore.WriteToIndex(currentLog, currentLog.getIndex());
            } else {
                prevLog = null;
                currentLog =  new Log(0, logStore.getCurrentTerm(), key, value);
                logStore.WriteToIndex(currentLog, currentLog.getIndex());
            }

            // create new sync object for current index
            for (int i = 0; i < clients.size(); i++) {
                syncObjects.get(i).put(currentLog.getIndex(), new Object());
                resultValidation.get(i).put(currentLog.getIndex(), 0);
            }


            lock.unlock();

            int currentLogIndex = currentLog.getIndex();
            CompletionService<Kvservice.APEResponse> completionService = new ExecutorCompletionService<>(executor);
            for (int i = 0; i < clients.size(); i++) {

                final int clientIdx = i;
                MyRunnable task = new MyRunnable(clientIdx, currentLogIndex, prevLog, currentLog);
                completionService.submit(task);
                /*-> {


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
                    Kvservice.APEResponse response = null;
                    try {
                         response = appendEntries(clients.get(clientIdx), prevLog, currentLog);
                    } catch (NoLongerLeaderException e) {
                        executor.shutdownNow();
                    }
                    // notify future put requests
                    Object currentSyncObject = syncObjects.get(clientIdx).get(currentLogIndex);
                    synchronized (currentSyncObject) {
                        syncObjects.get(clientIdx).remove(currentLogIndex);
                        currentSyncObject.notify();
                    }
                    return response;
                });*/
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
                        //this happens only when leaders term is less than followers
                        return;
                        // todo: handle failure
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                    // handle exception from server
                }
            }
            synchronized (this) {
                for(int index = logStore.getCommitIndex() + 1; index <= currentLogIndex; index++){
                    Optional<Log> log = logStore.ReadAtIndex(index);
                    kvStore.put(log.get().getKey(), log.get().getValue());
                }
                logStore.setCommitIndex(currentLogIndex);
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
                .setLeaderCommitIdx(logStore.getCommitIndex())
                .build();
        return request;

    }

    @Override
    public ScheduledExecutorService start() throws IOException{

        System.out.println("I am now a leader ! Current term : " + logStore.getCurrentTerm());
        System.out.println("Starting leader at - " + System.currentTimeMillis());

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(5);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                Kvservice.APERequest request = Kvservice.APERequest.newBuilder()
                        .setLeaderTerm(logStore.getCurrentTerm())
                        .setLeaderCommitIdx(logStore.getCommitIndex())
                        .setLeaderId(serverId)
                        .build();

            //todo :: may be add all required details

            CompletionService<Kvservice.APEResponse> completionService = new ExecutorCompletionService<>(executor);

                for (KVSClient client : clients) {
                    System.out.println("Sending heartbeat to client!!!");
                    completionService.submit(() -> {
                        Kvservice.APEResponse response;
                        response = client.appendEntries(request);
                        return response;
                    });
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }, 0, 5, TimeUnit.SECONDS);
        scheduledExecutor = scheduledExecutorService;
        return scheduledExecutorService;
    }

    @Override
    public void stop(ServiceType serviceType) {
        System.out.println("Stop called");
        newServiceType = serviceType;
        scheduledExecutor.shutdownNow();
    }

    @Override
    public ServiceType getType() {
        return ServiceType.LEADER;
    }

    @Override
    public Kvservice.APEResponse appendEntries(Kvservice.APERequest req) {
        try {
            int currentTerm = logStore.getCurrentTerm();
            if (req.getLeaderTerm() >= currentTerm) {
                System.out.println("Invalid call: Leader cannot receive append entries");
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
        try {
            int currentTerm = logStore.getCurrentTerm();
            if (req.getCandidateTerm() > currentTerm) {
                // update term
                logStore.setTerm(req.getCandidateTerm());
                stop(ServiceType.FOLLOWER); // return to follower state
                throw new Exception("Make the RPC call fail");
            }
            else {
                return Kvservice.RVResponse.newBuilder().setCurrentTerm(currentTerm).setVoteGranted(false).build();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

