package com.kv.service.grpc;

import com.kv.service.grpc.exception.HeartBeatMissedException;
import com.kv.store.LogStore;
import com.kv.store.LogStoreImpl;
import com.kvs.Kvservice;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import com.kvs.KVServiceGrpc;
import com.fasterxml.jackson.databind.ObjectMapper;


public class KVSServer {
    private static final Logger logger = Logger.getLogger(KVSServer.class.getName());
    private List<Map<String, Object>> servers;
    private Server server;

    private void start(ServiceType serviceType, int port) throws IOException {
        LogStore logStore = new LogStoreImpl("log" + port + ".txt");

        ReadAllServers(port);
        KVServiceFactory.instantiateClasses(serviceType, logStore, servers);
        server = ServerBuilder.forPort(port).addService(new KVSImpl()).build().start();

        // start
        while(true) {
            try {
                KVServiceFactory.getInstance().start();
            } catch (HeartBeatMissedException e) {
                // thrown when a follower does not receive messages from leader
                // Changing to candidate state
                KVServiceFactory.instantiateClasses(ServiceType.CANDIDATE, logStore, servers);

            } catch (Exception e) {
                // if any other unexpected exception
                break;
            }
        }

        logger.info("Server started, listening on " + port);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("Shutting down gRPC server");
                try {
                    server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
            }
        });
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

        KVService kvService;
        public KVSImpl() {
            this.kvService = KVServiceFactory.getInstance();
        }

        public void put(Kvservice.PutRequest req, StreamObserver<Kvservice.PutResponse> responseObserver) {
            logger.info("Got request from client: " + req);
            kvService.put(req.getKey(), req.getValue());

            Kvservice.PutResponse reply = Kvservice.PutResponse.newBuilder().setValue(
                    req.getValue()
            ).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        public void appendEntriesRPC(Kvservice.APERequest req, StreamObserver<Kvservice.APEResponse> responseObserver) {
            logger.info("Got request from client: " + req);
            Kvservice.APEResponse reply  = kvService.appendEntries(req);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }


    }

    static class KVServiceFactory {

        static KVService kvService;

        public static KVService getInstance() {
            return kvService;
        }

        public static void instantiateClasses(ServiceType type, LogStore logStore, List<Map<String, Object>> servers) throws IOException {
            switch (type){
                case LEADER:
                    kvService = new LeaderKVSService(logStore, servers);
                    break;
                case FOLLOWER:
                    kvService = new FollowerKVSService(logStore);
                    break;
                case CANDIDATE:
                    kvService = new CandidateKVSService(logStore, servers);
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

