package com.kv.service.grpc;

import com.kv.store.KVStore;
import com.kv.store.Log;
import com.kv.store.LogStore;
import com.kvs.Kvservice;
import com.kvs.Kvservice.APERequest;
import com.kvs.Kvservice.APEResponse;
import io.grpc.Metadata;
import io.grpc.Status;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;

public class FollowerKVSService extends KVService {

    // Zookeeper data
    private ZooKeeper zk;
    private String znodePath;
    private String currentNodeId;
    private final String ZNODE_PREFIX = "/election-";
    private final String ZNODE_PATH = "/election";
    private final int SESSION_TIMEOUT = 3000;


    long lastReceivedTS;
    private Object lockObject;

    public FollowerKVSService(LogStore logStore, KVStore kvStore, int port) {
        super(logStore, new ArrayList<String>(), kvStore, port);
        lastReceivedTS = System.currentTimeMillis();
        lockObject = new Object();
    }

    @Override
    public void put(int key, int value) {
        ThrowExceptionToRejectGetPut();
    }

//    @Override
//    public int get(int key) {
//        ThrowExceptionToRejectGetPut();
//        return 0;
//    }

    @Override
    public ScheduledExecutorService start() {
        try {
            System.out.println("I am a Follower ! My current term is : " + logStore.getCurrentTerm());
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }

        long threshhold = 10 * 1000;
        int period = 6;
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
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
            int leaderIndex  = req.getEntry(0).getIndex();
            int followerIndex = logStore.getNextIndex();

            if (followerIndex < leaderIndex) {
                return APEResponse.newBuilder().setIndex(followerIndex).setSuccess(false).build();
            }

            // todo: if (followerIndex > leaderIndex)

            // Write the new logs and apply to SM
            for (Kvservice.Entry entry : req.getEntryList()) {
                Log newLog = new Log(entry.getIndex(), -1, entry.getKey(), entry.getValue());
                logStore.WriteToIndex(newLog, followerIndex);
                kvStore.put(entry.getKey(), entry.getValue());
                followerIndex++;
            }

            return APEResponse.newBuilder().setIndex(followerIndex).setSuccess(true).build();
        } catch (IOException ex) {
            System.out.println("IO error");
            ex.printStackTrace();
            return APEResponse.newBuilder().setSuccess(false).build();
        }
    }

//    private void commitEntries(APERequest req) throws IOException {
//        if (req.getLeaderCommitIdx() > logStore.getCommitIndex()) {
//            Optional<Log> lastLog = logStore.getLastLogEntry();
//            int lastEntryIndex = lastLog.map(Log::getIndex).orElse(-1);
//            int newCommitIndex = Math.min(req.getLeaderCommitIdx(), lastEntryIndex);
//            // apply to state machine
//            for (int i = logStore.getCommitIndex() + 1; i <= newCommitIndex; i++) {
//                Log log = logStore.ReadAtIndex(i).get();
//                kvStore.put(log.getKey(), log.getValue());
//            }
//            logStore.setCommitIndex(newCommitIndex);
//        }
//    }

    @Override
    public Kvservice.RVResponse requestVotes(Kvservice.RVRequest req) {
        Kvservice.RVResponse response = super.requestVotes(req);
        if (response.getVoteGranted()) {
            lastReceivedTS = System.currentTimeMillis();
        }

        return response;
    }
}
