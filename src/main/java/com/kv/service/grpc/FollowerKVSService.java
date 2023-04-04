package com.kv.service.grpc;

import com.kv.store.KVStore;
import com.kv.store.Log;
import com.kv.store.LogStore;
import com.kvs.Kvservice;
import com.kvs.Kvservice.APERequest;
import com.kvs.Kvservice.APEResponse;
import io.grpc.Metadata;
import io.grpc.Status;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;

public class FollowerKVSService extends KVService {

    long lastReceivedTS;

    public FollowerKVSService(LogStore logStore, KVStore kvStore, int port) {
        super(logStore, new ArrayList<Map<String, Object>>(), kvStore, port);
        lastReceivedTS = System.currentTimeMillis();
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
        long threshhold = 7 * 1000;
        int period = 6;
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(() -> {
                    if (System.currentTimeMillis() - lastReceivedTS > threshhold) {
                        stop(ServiceType.CANDIDATE);
                    }
                }, (int) (5 + (Math.random() * (7 - 5))), period
                , TimeUnit.SECONDS);
        scheduledExecutor = executor;
        return executor;
    }

    @Override
    public void stop(ServiceType serviceType) {
        newServiceType = serviceType;
        scheduledExecutor.shutdownNow();
    }

    @Override
    public ServiceType getType() {
        return ServiceType.FOLLOWER;
    }

    @Override
    public APEResponse appendEntries(APERequest req) {
        try {
            synchronized (this) {
                lastReceivedTS = System.currentTimeMillis();
            }
            if (req.getEntryList() == null) {
                // Heart beat request
                //todo :: may be commit entries and term?
                return APEResponse.newBuilder().setSuccess(true).build();
            }
            // Case 1: compare terms
            int currentTerm = logStore.getCurrentTerm();
            if (req.getLeaderTerm() < currentTerm) {
                logger.error("leader term less than current term of follower");
                return APEResponse.newBuilder().setCurrentTerm(currentTerm).setSuccess(false).build();
            }

            // update currentTerm to latest term seen from leader
            if (currentTerm != req.getLeaderTerm()) {
                logStore.setTerm(req.getLeaderTerm());
                currentTerm = req.getLeaderTerm();
            }

            // Case 2: if NOT the first log from the leader / previous log data exists in leader
            if (req.getPrevLogIndex() != -1) {
                Optional<Log> prevLog = logStore.ReadAtIndex(req.getPrevLogIndex());
                if (!(prevLog.isPresent() && req.getPrevLogTerm() == prevLog.get().getTerm() && req.getPrevLogIndex() == prevLog.get().getIndex())) {
                    logger.error("previous log entry does not match");
                    return APEResponse.newBuilder().setCurrentTerm(currentTerm).setSuccess(false).build();
                }
            }

            Kvservice.Entry logEntry = req.getEntry(0);
            int currentIndex = logEntry.getIndex();
            // Case 3: if existing entry does not match delete the existing entry and all that follow it
            Optional<Log> currentLog = logStore.ReadAtIndex(currentIndex);
            if (currentLog.isPresent() && !(logEntry.getIndex() == currentLog.get().getIndex()
                    && logEntry.getTerm() == currentLog.get().getTerm()
                    && logEntry.getKey() == currentLog.get().getKey()
                    && logEntry.getValue() == currentLog.get().getValue())) {
                // mark -1s
                logStore.markEnding(currentIndex);
            }

            // Write the new logs
            for (Kvservice.Entry entry : req.getEntryList()) {
                Log newLog = new Log(entry.getIndex(), entry.getTerm(), entry.getKey(), entry.getValue());
                logStore.WriteToIndex(newLog, currentIndex);
                currentIndex++;
            }

            // commit
            if (req.getLeaderCommitIdx() > logStore.getCommitIndex()) {
                Optional<Log> lastLog = logStore.getLastLogEntry();
                int lastEntryIndex = lastLog.map(Log::getIndex).orElse(-1);
                int newCommitIndex = Math.min(req.getLeaderCommitIdx(), lastEntryIndex);
                // apply to state machine
                for (int i = logStore.getCommitIndex() + 1; i <= newCommitIndex; i++) {
                    Log log = logStore.ReadAtIndex(i).get();
                    kvStore.put(log.getKey(), log.getValue());
                }
                logStore.setCommitIndex(newCommitIndex);
            }

            return APEResponse.newBuilder().setCurrentTerm(currentTerm).setSuccess(true).build();
        } catch (IOException ex) {
            logger.error("IO error");
            ex.printStackTrace();
            return APEResponse.newBuilder().setSuccess(false).build();
        }
    }

    @Override
    public Kvservice.RVResponse requestVotes(Kvservice.RVRequest req) {
        return null;
    }
}
