//package com.kv.service.grpc;
//
//import com.kv.store.KVStore;
//import com.kv.store.Log;
//import com.kv.store.LogStore;
//import com.kvs.Kvservice;
//import io.grpc.Metadata;
//import io.grpc.Status;
//
//import java.io.IOException;
//import java.util.List;
//import java.util.Map;
//import java.util.Optional;
//import java.util.concurrent.*;
//
//public class CandidateKVSService extends KVService{
//
//    List<Map<String, Object>> servers;
//
//    private ExecutorService executor;
//    public CandidateKVSService(LogStore logStore,  List<Map<String, Object>> servers, KVStore kvStore, int port) {
//        super(logStore, servers, kvStore, port);
//        this.servers = servers;
//        executor = Executors.newFixedThreadPool(5);
//    }
//
//    @Override
//    public void put(int key, int value) {
//        ThrowExceptionToRejectGetPut();
//    }
//
//    @Override
//    public int get(int key) {
//        ThrowExceptionToRejectGetPut();
//        return 0;
//    }
//
//    @Override
//    public ScheduledExecutorService start() {
//        try {
//            logStore.setTerm(logStore.getCurrentTerm() + 1);
//            System.out.println(("I am now a candidate ! My current term is : " + logStore.getCurrentTerm()));
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//
//        ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(2);
//        scheduled.schedule(() -> {
//            try {
//                if (!logStore.getVotedFor().isPresent()) {
//                    System.out.println("Voting for myself for term : " + logStore.getCurrentTerm());
//                    logStore.setVotedFor(serverId);
//                }
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//            scheduled.schedule(()-> {
//                stop(ServiceType.CANDIDATE);
//            }, 7, TimeUnit.SECONDS);
//            CompletionService<Kvservice.RVResponse> completionService = new ExecutorCompletionService<>(executor);
//            try {
//                Kvservice.RVRequest request = getRVRequest();
//                for (KVSClient client : clients) {
//                    completionService.submit(() -> {
//                        Kvservice.RVResponse response = client.requestVote(request);
//                        return response;
//                    });
//
//                }
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//            int voteCount = 1;
//            int totalServers = clients.size() + 1;
//            while (voteCount <= totalServers/2) {
//                try {
//                    Future<Kvservice.RVResponse> completedTask = completionService.take();
//                    if(completedTask.get() != null) {
//                        if (completedTask.get().getVoteGranted()) {
//                            voteCount++;
//                        } else {
//                            if (completedTask.get().getCurrentTerm() > logStore.getCurrentTerm()) {
//                                // follower term is greater than the candidate
//                                logStore.setTerm(completedTask.get().getCurrentTerm());
//                                stop(ServiceType.CANDIDATE);
//                            }
//                        }
//                    }
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                } catch (ExecutionException e) {
//                    throw new RuntimeException(e);
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//
//            }
//            // received votes change to leader
//            stop(ServiceType.LEADER);
//
//        }, (long)Math.random()%5, TimeUnit.SECONDS);
//        scheduledExecutor = scheduled;
//
//        return scheduled;
//    }
//
//    private Kvservice.RVRequest getRVRequest() throws IOException {
//        Optional<Log> log = logStore.getLastLogEntry();
//        Kvservice.RVRequest request = Kvservice.RVRequest.newBuilder()
//                .setCandidateId(serverId)
//                .setCandidateTerm(logStore.getCurrentTerm())
//                .setLastLogIndex(log.isPresent()? log.get().getIndex() : -1)
//                .setLastLogTerm(log.isPresent() ? log.get().getTerm() : -1)
//                .build();
//        return request;
//    }
//
//    @Override
//    public void stop(ServiceType serviceType) {
//        newServiceType = serviceType;
//        scheduledExecutor.shutdownNow();
//    }
//
//    public com.kvs.Kvservice.APEResponse appendEntries(com.kvs.Kvservice.APERequest req) {
//        try {
//            int currentTerm = logStore.getCurrentTerm();
//            if (req.getLeaderTerm() >= currentTerm) {
//                // update term
//                logStore.setTerm(req.getLeaderTerm());
//                if (req.getEntryList() == null || req.getEntryList().size() == 0) leaderId = req.getLeaderId();
//                stop(ServiceType.FOLLOWER); // return to follower state
//                throw new Exception("Make the RPC call fail");
//            }
//            else {
//                return Kvservice.APEResponse.newBuilder().setCurrentTerm(currentTerm).setSuccess(false).build();
//            }
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    @Override
//    public ServiceType getType() {
//        return ServiceType.CANDIDATE;
//    }
//}
