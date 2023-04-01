package com.kv.service.grpc;

import com.kv.store.Log;
import com.kv.store.LogStore;
import com.kvs.Kvservice;
import com.kvs.Kvservice.APERequest;
import com.kvs.Kvservice.APEResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class FollowerKVSService extends KVService {
    public FollowerKVSService(LogStore logStore) {
        super(logStore, new ArrayList<Map<String, Object>>());
    }

    @Override
    public void put(int key, int value) {

    }

    @Override
    public void start() {

    }

    @Override
    public ServiceType getType() {
        return ServiceType.FOLLOWER;
    }

    @Override
    public APEResponse appendEntries(APERequest req) {
        try {

            // Case 1: compare terms
            int currentTerm = logStore.getCurrentTerm(); //todo: can maintain local state
            if (req.getLeaderTerm() < currentTerm) {
                logger.error("leader term less than current term of follower");
                return APEResponse.newBuilder().setCurrentTerm(currentTerm).setSuccess(false).build();
            }

            // update currentTerm to latest term seen from leader
            currentTerm = req.getLeaderTerm();
            logStore.setTerm(currentTerm);

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

            // todo: check commit index

            // Write the new logs
            for (Kvservice.Entry entry : req.getEntryList()) {
                Log newLog = new Log(entry.getIndex(), entry.getTerm(), entry.getKey(), entry.getValue());
                logStore.WriteToIndex(newLog, currentIndex);
                currentIndex++;
            }
            return APEResponse.newBuilder().setCurrentTerm(currentTerm).setSuccess(true).build();
        } catch (IOException ex) {
            logger.error("IO error");
            ex.printStackTrace();
            return APEResponse.newBuilder().setSuccess(false).build();
        }
    }
}
