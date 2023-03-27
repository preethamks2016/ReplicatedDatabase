package com.kv.service.grpc;

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

    public KVService service;

    private void start() throws IOException {
        int port = 50051;
        KVServiceFactory.instantiateClasses(ServiceTYpe.FOLLOWER);

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


        public void put(Kvservice.PutRequest req, StreamObserver<Kvservice.PutResponse> responseObserver) {
            KVService kvService = KVServiceFactory.getInstance();
            kvService.put(req.getKey(), req.getValue());

            logger.info("Got request from client: " + req);
            Kvservice.PutResponse reply = Kvservice.PutResponse.newBuilder().setValue(
                    req.getValue()
            ).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }

    static class KVServiceFactory {

        static KVService kvService;
        public static KVService getInstance() {
            return kvService;
        }

        public static void instantiateClasses(ServiceTYpe type) {
            switch (type){
                case LEADER:
                    kvService = new LeaderKVSService();
                    break;
                case FOLLOWER:
                    kvService = new FollowerKVSService();
                    break;
                case CANDIDATE:
                    kvService = new CandidateKVSService();
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

