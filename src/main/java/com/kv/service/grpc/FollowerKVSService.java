package com.kv.service.grpc;

import com.kv.service.grpc.exception.HeartBeatMissedException;
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

    long lastReceivedTS;

    public FollowerKVSService(LogStore logStore) {
        super(logStore, new ArrayList<Map<String, Object>>());
        lastReceivedTS = System.currentTimeMillis();
    }

    @Override
    public void put(int key, int value) {

    }

    @Override
    public void start() throws HeartBeatMissedException {
        long threshhold = 7 * 1000;
        while (true) {
            if(System.currentTimeMillis() - lastReceivedTS > threshhold) {
                //todo :: increase term
                throw new HeartBeatMissedException();
                // leader election;
            }
        }

    }

    @Override
    public void stop() {

    }

    @Override
    public ServiceType getType() {
        return ServiceType.FOLLOWER;
    }

    @Override
    public APEResponse appendEntries(APERequest req) {
        try {
            synchronized(this) {
                lastReceivedTS = System.currentTimeMillis();
            }
            if(req.getEntryList() == null) {
                // Heart beat request
                return APEResponse.newBuilder().setSuccess(true).build();
            }
            if (req.getPrevLogIndex() != -1) {
                //todo: can maintain local state
                int currentTerm = getCurrentTerm();

                if (req.getLeaderTerm() < currentTerm) {
                    logger.error("leader term less than current term of follower");
                    return APEResponse.newBuilder().setCurrentTerm(currentTerm).setSuccess(false).build();
                }

                Log prevLog = logStore.ReadAtIndex(req.getPrevLogIndex());
                if (prevLog == null || req.getPrevLogTerm() != prevLog.getTerm()) {
                    logger.error("previous log entry does not match");
                    return APEResponse.newBuilder().setCurrentTerm(currentTerm).setSuccess(false).build();
                }

                // todo: if existing entry does not match delete the existing entry and all that follow it

                // todo: check commit index
            }

            // write the new log
            Kvservice.Entry logEntry = req.getEntry(0);
            Log newLog = new Log(logEntry.getIndex(), logEntry.getTerm(), logEntry.getKey(), logEntry.getValue());
            logStore.WriteToIndex(newLog, logEntry.getIndex());
            return APEResponse.newBuilder().setCurrentTerm(req.getLeaderTerm()).setSuccess(true).build();
        } catch (IOException ex) {
            logger.error("IO error");
            ex.printStackTrace();
            return APEResponse.newBuilder().setSuccess(false).build();
        }
    }

    // gets current term by reading the last log in the log list
    int getCurrentTerm() throws IOException {
        Optional<Log> optionalLog = logStore.getLastLogEntry();
        return optionalLog.map(Log::getTerm).orElse(-1);
    }
}
