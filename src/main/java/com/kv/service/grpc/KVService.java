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

    protected int serverId;

    protected int leaderId;

    public KVService (LogStore logStore, List<String> servers, KVStore kvStore, int port) {
//        String serverAddress = "localhost:50051";
//        ManagedChannel channel = ManagedChannelBuilder.forTarget(serverAddress)
//                .usePlaintext()
//                .build();
//        this.client = new KVSClient(channel);
        serverId = port;
        leaderId = -1;
        clients = new ArrayList<KVSClient>();
        this.kvStore = kvStore;
        for (String server : servers) {
            ManagedChannel channel = ManagedChannelBuilder.forTarget(server)
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

    public abstract Kvservice.PutResponse put(int key, int value);

//    public abstract Kvservice.GetResponse get(int key) throws IOException;

    public Kvservice.GetResponse get(int key) throws IOException {
        return Kvservice.GetResponse.newBuilder()
                .setIndex(logStore.getNextIndex())
                .setValue(this.kvStore.get(key))
                .build();
    }

    public abstract ScheduledExecutorService start() throws IOException;

    public abstract void stop(ServiceType serviceType);

    public abstract ServiceType getType();

    public abstract Kvservice.APEResponse appendEntries(Kvservice.APERequest req);

    public void ThrowExceptionToRejectGetPut() {
        Status status = Status.UNAVAILABLE.withDescription("I am not the leader");
        System.out.println("The leader is at port " + leaderId);
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("leaderPort", Metadata.ASCII_STRING_MARSHALLER), Integer.toString(leaderId));
        throw status.asRuntimeException(metadata);
    }

    public Kvservice.RVResponse requestVotes(Kvservice.RVRequest req) {
        try {
            int currentTerm = logStore.getCurrentTerm();
            if (req.getCandidateTerm() < currentTerm) {
                System.out.println("Voted false. Current term is greater than candidate term.");
                return Kvservice.RVResponse.newBuilder().setVoteGranted(false).setCurrentTerm(currentTerm).build();
            } else if (req.getCandidateTerm() > currentTerm) {
                System.out.println("Voted true. Got greater term in request than current term " + currentTerm);
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
                    System.out.println("Did not vote. Already voted for term " + currentTerm);
                    return Kvservice.RVResponse.newBuilder().setVoteGranted(false).setCurrentTerm(currentTerm).build();
                } else {
                    // vote
                    Optional<Log> logOptional = logStore.getLastLogEntry();
                    if (!logOptional.isPresent()) {
                        System.out.println("Case1: Vote granted for term " + currentTerm);
                        logStore.setVotedFor(req.getCandidateId());
                        return Kvservice.RVResponse.newBuilder().setVoteGranted(true).setCurrentTerm(currentTerm).build();
                    } else if (logOptional.get().getTerm() <= req.getLastLogTerm() && logOptional.get().getIndex() <= req.getLastLogIndex()) {
                        logStore.setVotedFor(req.getCandidateId());
                        System.out.println("Vote granted for term " + currentTerm);
                        return Kvservice.RVResponse.newBuilder().setVoteGranted(true).setCurrentTerm(currentTerm).build();
                    } else {
                        System.out.println("Voted false. My log is more up to date. ");
                        return Kvservice.RVResponse.newBuilder().setVoteGranted(false).setCurrentTerm(currentTerm).build();
                    }
                }
            }
        } catch (IOException ex) {
            System.out.println("IO error");
            throw new RuntimeException(ex);
        }
    }
}
