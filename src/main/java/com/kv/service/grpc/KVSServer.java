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
    private void start() throws IOException {
        int port = 50051;
        server = ServerBuilder.forPort(port).addService(new KVSImpl()).build().start();

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
            logger.info("Got request from client: " + req);
            Kvservice.PutResponse reply = Kvservice.PutResponse.newBuilder().setValue(
                    req.getValue()
            ).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException {
        final KVSServer kvs = new KVSServer();
        kvs.start();
        kvs.server.awaitTermination();
    }
}

