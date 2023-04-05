package com.kv.service.grpc;

import com.kv.store.KVStore;
import com.kv.store.Log;
import com.kv.store.LogStore;
import com.kvs.Kvservice;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.security.Provider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

public abstract class KVService {

//    protected KVSClient client;
    protected LogStore logStore;
    protected Logger logger;
    protected List<KVSClient> clients;

    protected KVStore kvStore;



    protected ScheduledExecutorService scheduledExecutor;

    protected ServiceType newServiceType;

    public KVService (LogStore logStore, List<Map<String, Object>> servers, KVStore kvStore) {
//        String serverAddress = "localhost:50051";
//        ManagedChannel channel = ManagedChannelBuilder.forTarget(serverAddress)
//                .usePlaintext()
//                .build();
//        this.client = new KVSClient(channel);
        clients = new ArrayList<KVSClient>();
        this.kvStore = kvStore;
        for (Map<String, Object> server : servers) {
            ManagedChannel channel = ManagedChannelBuilder.forTarget(server.get("ip").toString() + ":" + server.get("port").toString())
                                        .usePlaintext()
                                        .enableRetry()
                                        .maxRetryAttempts(10000)
                                        .build();

            KVSClient client = new KVSClient(channel);
            clients.add(client);
        }

        this.logStore = logStore;
        this.logger = LogManager.getLogger(KVService.class);
        BasicConfigurator.configure();
    }

    public abstract void put(int key, int value);
    public abstract int get(int key);
    public abstract ScheduledExecutorService start();

    public abstract void stop(ServiceType newType);

    public abstract ServiceType getType();

    public abstract Kvservice.APEResponse appendEntries(Kvservice.APERequest req);

    public void ThrowExceptionToRejectGetPut() {
        Status status = Status.UNAVAILABLE.withDescription("I am not the leader");
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("ip", Metadata.ASCII_STRING_MARSHALLER), "SomeIP");
        metadata.put(Metadata.Key.of("port", Metadata.ASCII_STRING_MARSHALLER), "SomePort");
        throw status.asRuntimeException();
    }

    public Kvservice.RVResponse requestVotes(Kvservice.RVRequest req) {
        try {
            int currentTerm = logStore.getCurrentTerm();
            if (req.getCandidateTerm() < currentTerm) {
                return Kvservice.RVResponse.newBuilder().setVoteGranted(false).setCurrentTerm(currentTerm).build();
            } else if (req.getCandidateTerm() > currentTerm) {
                logStore.setTerm(req.getCandidateTerm());
                logStore.setVotedFor(req.getCandidateId());
                currentTerm = logStore.getCurrentTerm();
                return Kvservice.RVResponse.newBuilder().setVoteGranted(true).setCurrentTerm(currentTerm).build();
            } else
            // the current terms are equal
            {
                Optional<Integer> votedFor = logStore.getVotedFor();
                if (votedFor.isPresent()) {
                    // already voted
                    return Kvservice.RVResponse.newBuilder().setVoteGranted(false).setCurrentTerm(currentTerm).build();
                } else {
                    // vote
                    Optional<Log> logOptional = logStore.getLastLogEntry();
                    if (!logOptional.isPresent()) {
                        logStore.setVotedFor(req.getCandidateId());
                        return Kvservice.RVResponse.newBuilder().setVoteGranted(true).setCurrentTerm(currentTerm).build();
                    } else if (logOptional.get().getTerm() <= req.getLastLogTerm() && logOptional.get().getIndex() <= req.getLastLogIndex()) {
                        logStore.setVotedFor(req.getCandidateId());
                        return Kvservice.RVResponse.newBuilder().setVoteGranted(true).setCurrentTerm(currentTerm).build();
                    } else {
                        return Kvservice.RVResponse.newBuilder().setVoteGranted(false).setCurrentTerm(currentTerm).build();
                    }
                }
            }
        } catch (IOException ex) {
            logger.error("IO error");
            throw new RuntimeException(ex);
        }
    }
}
