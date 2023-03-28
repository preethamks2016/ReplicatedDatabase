package com.kv.service.grpc;

import com.kv.store.LogStore;
import com.kv.store.LogStoreImpl;
import com.kvs.Kvservice;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import com.kvs.KVServiceGrpc;


public class KVSServer {
    private static final Logger logger = Logger.getLogger(KVSServer.class.getName());
    private Server server;

    private void start() throws IOException {
        int port = 50051;

        KVServiceFactory.instantiateClasses(ServiceType.FOLLOWER);

        server = ServerBuilder.forPort(port).addService(new KVSImpl()).build().start();

        // start
        try {
            KVServiceFactory.getInstance().start();
        } catch (Exception e) {

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
            Kvservice.APEResponse reply = null;
            if (kvService.getType().equals(ServiceType.FOLLOWER)) {
                reply = ((FollowerKVSService) kvService).appendEntries(req);
            } else {
                // error: only follower can append entries
            }
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }


    }

    static class KVServiceFactory {

        static KVService kvService;
        public static KVService getInstance() {
            return kvService;
        }

        public static void instantiateClasses(ServiceType type) throws IOException {
            LogStore logStore = new LogStoreImpl("log.txt");
            switch (type){
                case LEADER:
                    kvService = new LeaderKVSService(logStore);
                    break;
                case FOLLOWER:
                    kvService = new FollowerKVSService(logStore);
                    break;
                case CANDIDATE:
                    kvService = new CandidateKVSService(logStore);
                    break;
            }
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException {
        final KVSServer kvs = new KVSServer();
        kvs.start();
        kvs.server.awaitTermination();
    }

}

