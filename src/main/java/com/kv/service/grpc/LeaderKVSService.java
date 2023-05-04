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

    ReentrantLock lock;
    LeaderKVSService(LogStore logStore, List<String> servers, KVStore kvStore, int port) {
        super(logStore, servers, kvStore, port);
        lock = new ReentrantLock();
        executor = Executors.newFixedThreadPool(5);
        syncObjects = new ArrayList<ConcurrentHashMap<Integer, Object>>();
        resultValidation = new ArrayList<ConcurrentHashMap<Integer, Integer>>();
        for (int i = 0; i < clients.size(); i++) {
            syncObjects.add(new ConcurrentHashMap<Integer, Object>());
            resultValidation.add(new ConcurrentHashMap<Integer, Integer>());
        }
    }

    public Kvservice.APEResponse appendEntries(KVSClient client, Log currentLog) throws IOException, NoLongerLeaderException {
        Kvservice.APERequest request =  populateAPERequest(currentLog);
        Kvservice.APEResponse response = client.appendEntries(request);
        // handle failures / mismatches
        while (!response.getSuccess() && response.getIndex()<currentLog.getIndex()) {
            List<Kvservice.Entry> entries = new ArrayList<>();
            for (int index = response.getIndex(); index <= currentLog.getIndex(); index++) {
                entries.add(getRequestEntry(logStore.ReadAtIndex(index).get()));
            }
            request = setPrevLogEntries(response.getIndex(), entries);
            response = client.appendEntries(request);
        }

//        while (!response.getSuccess()) {
//            if(response.getCurrentTerm() > request.getLeaderTerm()) {
//                stop(ServiceType.FOLLOWER);
//                throw new NoLongerLeaderException(0);
//                //todo:: should stop execution??
//                // follower term is greater than leader : fall back to follower state
//            } else {
//                // try comparing prev index - 1
//                List<Kvservice.Entry> entries = new ArrayList<>();
//                entries.add(getRequestEntry(prevLog));
//                entries.addAll(request.getEntryList());
//                if (prevLog.getIndex() != 0) {
//                    Optional<Log> optionalLog = logStore.ReadAtIndex(prevLog.getIndex() - 1);
//                    prevLog = optionalLog.get();
//                }
//                else {
//                    prevLog = null;
//                }
//
//                request = setPrevLogEntries(request, prevLog, entries);
//                response = client.appendEntries(request);
//            }
//        }
        return response;
    }

    private Kvservice.APERequest setPrevLogEntries(int index, List<Kvservice.Entry> entries) {
        return Kvservice.APERequest.newBuilder()
                .setIndex(index)
                .addAllEntry(entries)
                .build();
    }

    private Kvservice.Entry getRequestEntry(Log log) {
        return Kvservice.Entry.newBuilder()
                .setIndex(log.getIndex())
                .setKey(log.getKey())
                .setValue(log.getValue()).build();
    }

    public class MyRunnable implements Callable<Kvservice.APEResponse> {

        private final int clientIdx;
        private final int currentLogIndex;

        private final Log currentLog;

        public MyRunnable( int clientIndex, int currentLogIndex, Log currentLog) {
            this.clientIdx = clientIndex;
            this.currentLogIndex = currentLogIndex;
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
                response = appendEntries(clients.get(clientIdx), currentLog);
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

            // write to own log
            int idx = logStore.getNextIndex();
            Log log =  new Log(idx, -1, key, value); // term is not required here
            logStore.WriteToIndex(log, idx);

            // write to levelDB
            kvStore.put(key, value);

            // create new sync object for current index
            for (int i = 0; i < clients.size(); i++) {
                syncObjects.get(i).put(idx, new Object());
                resultValidation.get(i).put(idx, 0);
            }

            lock.unlock();

            CompletionService<Kvservice.APEResponse> completionService = new ExecutorCompletionService<>(executor);
            for (int i = 0; i < clients.size(); i++) {

                final int clientIdx = i;
                MyRunnable task = new MyRunnable(clientIdx, idx, log);
                completionService.submit(task);
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
                        // this can happen if a follower is unreachable or not upto date
                        // code can get stuck in an infinite loop here if majority nodes are unreachable (since retries number is fixed)
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
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

//    @Override
//    public Kvservice.GetResponse get(int key) throws IOException {
//        return Kvservice.GetResponse.newBuilder()
//                .setIndex(logStore.getNextIndex())
//                .setValue(this.kvStore.get(key))
//                .build();
//    }

    private Kvservice.APERequest populateAPERequest(Log currentLog) {

        List<Kvservice.Entry> entries = new ArrayList<>();
        Kvservice.Entry entry = Kvservice.Entry.newBuilder()
                .setIndex(currentLog.getIndex())
                .setKey(currentLog.getKey())
                .setValue(currentLog.getValue()).build();
        entries.add(entry);

        return Kvservice.APERequest.newBuilder()
                .setIndex(currentLog.getIndex())
                .addAllEntry(entries)
                .build();

    }

    @Override
    public ScheduledExecutorService start() throws IOException{

        System.out.println("I am now a leader ! Current term : " + logStore.getCurrentTerm());
        System.out.println("Starting leader at - " + System.currentTimeMillis());

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(5);
//        scheduledExecutorService.scheduleAtFixedRate(() -> {
//            try {
//                Kvservice.APERequest request = Kvservice.APERequest.newBuilder()
//                        .build();
//
//            //todo :: may be add all required details
//
//            CompletionService<Kvservice.APEResponse> completionService = new ExecutorCompletionService<>(executor);
//
//                for (KVSClient client : clients) {
//                    System.out.println("Sending heartbeat to client!!!");
//                    completionService.submit(() -> {
//                        Kvservice.APEResponse response;
//                        response = client.appendEntries(request);
//                        return response;
//                    });
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//                throw new RuntimeException(e);
//            }
//        }, 0, 5, TimeUnit.SECONDS);
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
        // Leader cannot receive append entries
        return Kvservice.APEResponse.newBuilder().setSuccess(false).build();
//        try {
//            int currentTerm = logStore.getCurrentTerm();
//            if (req.getLeaderTerm() >= currentTerm) {
//                System.out.println("Invalid call: Leader cannot receive append entries");
//                // update term
//                logStore.setTerm(req.getLeaderTerm());
//                stop(ServiceType.FOLLOWER); // return to follower state
//                throw new Exception("Make the RPC call fail");
//            }
//            else {
//                return Kvservice.APEResponse.newBuilder().setCurrentTerm(currentTerm).setSuccess(false).build();
//            }
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
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

