package com.kv.service.grpc;

import com.kvs.KVServiceGrpc;
import com.kvs.Kvservice;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;


public class KVSClient {
    private static final Logger logger = Logger.getLogger(KVSClient.class.getName());
    private final KVServiceGrpc.KVServiceBlockingStub blockingStub;

    public KVSClient(Channel channel) {
        blockingStub = KVServiceGrpc.newBlockingStub(channel);
    }
    public void put(int key, int value) {

        Kvservice.PutRequest request = Kvservice.PutRequest.newBuilder().build().newBuilder().setKey(key).setValue(value).build();
        logger.info("Sending to server: " + request);
        Kvservice.PutResponse response;
        try {
            response = blockingStub.put(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Got following from the server: " + response.getValue());
    }

    public static void main(String[] args) throws Exception {
        int key = 1;
        int val = 2;
        String serverAddress = "localhost:50051";
        ManagedChannel channel = ManagedChannelBuilder.forTarget(serverAddress)
                .usePlaintext()
                .build();
        try {
            KVSClient client = new KVSClient(channel);
            client.put(key, val);
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
