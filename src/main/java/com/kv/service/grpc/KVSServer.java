package com.kv.service.grpc;

import com.kv.store.KVStore;
import com.kv.store.KVStoreImpl;
import com.kv.store.LogStore;
import com.kv.store.LogStoreImpl;
import com.kvs.Kvservice;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import com.kvs.KVServiceGrpc;
import com.fasterxml.jackson.databind.ObjectMapper;


public class KVSServer {
    private static final Logger logger = Logger.getLogger(KVSServer.class.getName());
    private List<Map<String, Object>> servers;
    private Server server;

    private void start(ServiceType serviceType, int port) throws IOException {
        LogStore logStore = new LogStoreImpl("log" + port + ".txt", "meta" + port + ".txt");
        KVStore kvStore = new KVStoreImpl();
        ReadAllServers(port);
        KVServiceFactory.instantiateClasses(serviceType, logStore, servers, kvStore, port);
        server = ServerBuilder.forPort(port).addService(new KVSImpl()).build().start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("Shutting down gRPC server");
                try {
                    System.out.println("Shutting down at - " + System.currentTimeMillis());
                    server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
            }
        });

        while (true) {
            try {
                System.out.println("The Server is now starting !");
                ScheduledExecutorService scheduledExecutor = KVServiceFactory.getInstance().start();
                //wait for the scheduled executor to end
                scheduledExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                System.out.println("Executor is terminated");
                System.out.println("changing from - " +serviceType + " to - " + KVServiceFactory.getInstance().newServiceType);
                serviceType = KVServiceFactory.getInstance().newServiceType;
                //instantiate new class
                KVServiceFactory.instantiateClasses(serviceType, logStore, servers, kvStore, port);
            } catch (Exception e) {

            }
        }


    }

    private void ReadAllServers(int selfPort) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        File configFile = new File("servers.json");
        Map<String, Object> configMap = objectMapper.readValue(configFile, Map.class);
        List<Map<String, Object>> allServers = (List<Map<String, Object>>) configMap.get("servers");

        servers = new ArrayList<Map<String, Object>>();
        for (Map<String, Object> serverMap : allServers) {
            int port = (int) serverMap.get("port");
            if (port != selfPort) {
                servers.add(serverMap);
            }
        }
    }

    static class KVSImpl extends KVServiceGrpc.KVServiceImplBase {


        public void put(Kvservice.PutRequest req, StreamObserver<Kvservice.PutResponse> responseObserver) {
            System.out.println("Got request from client: " + req);

            try {
                KVServiceFactory.getInstance().put(req.getKey(), req.getValue());
                Kvservice.PutResponse reply = Kvservice.PutResponse.newBuilder().setValue(
                        req.getValue()
                ).build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            }
            catch (StatusRuntimeException ex) {
                responseObserver.onError(ex);
            }
        }

        public void get(Kvservice.GetRequest req, StreamObserver<Kvservice.GetResponse> responseObserver) {
            System.out.println("Got request from client: " + req);

            try {
                int response = KVServiceFactory.getInstance().get(req.getKey());
                Kvservice.GetResponse reply = Kvservice.GetResponse.newBuilder().setValue(
                        response
                ).build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            }
            catch (StatusRuntimeException ex) {
                responseObserver.onError(ex);
            }
        }

        public void appendEntriesRPC(Kvservice.APERequest req, StreamObserver<Kvservice.APEResponse> responseObserver) {
            System.out.println("Got request from client: index:" + (req.getPrevLogIndex()+1) + ", nEntries: " + req.getEntryList().size());
            try {
                Kvservice.APEResponse reply = KVServiceFactory.getInstance().appendEntries(req);
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            }
            catch (Exception ex) {
                responseObserver.onError(Status.FAILED_PRECONDITION.withDescription("Failed check").asRuntimeException());
            }
        }

        public void requestVoteRPC(Kvservice.RVRequest req, StreamObserver<Kvservice.RVResponse> responseObserver) {
            System.out.println("Got request from client: " + req);
            try {
                Kvservice.RVResponse reply = KVServiceFactory.getInstance().requestVotes(req);
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            }
            catch (Exception ex) {
                responseObserver.onError(Status.FAILED_PRECONDITION.withDescription("Failed check").asRuntimeException());
            }
        }


    }

    static class KVServiceFactory {

        static KVService kvService;

        public static KVService getInstance() {
            return kvService;
        }

        public static void instantiateClasses(ServiceType type, LogStore logStore, List<Map<String, Object>> servers, KVStore kvStore, int port) throws IOException {
            switch (type){
                case LEADER:
                    kvService = new LeaderKVSService(logStore, servers, kvStore, port);
                    break;
                case FOLLOWER:
                    kvService = new FollowerKVSService(logStore, kvStore, port);
                    break;
                case CANDIDATE:
                    kvService = new CandidateKVSService(logStore, servers, kvStore, port);
                    break;
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final KVSServer kvs = new KVSServer();
        kvs.start(ServiceType.valueOf(args[0]), Integer.valueOf(args[1]));
        kvs.server.awaitTermination();
    }

}
