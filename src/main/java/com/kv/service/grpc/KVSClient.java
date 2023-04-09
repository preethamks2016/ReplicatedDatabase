package com.kv.service.grpc;

import com.kv.service.grpc.exception.NoLongerLeaderException;
import com.kvs.KVServiceGrpc;
import com.kvs.Kvservice;
import io.grpc.*;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;


public class KVSClient {
    private static final Logger logger = Logger.getLogger(KVSClient.class.getName());
    private final KVServiceGrpc.KVServiceBlockingStub blockingStub;

    public KVSClient(Channel channel) {
        blockingStub = KVServiceGrpc.newBlockingStub(channel);
    }
    public void put(int key, int value) throws NoLongerLeaderException {
        Kvservice.PutRequest request = Kvservice.PutRequest.newBuilder().setKey(key).setValue(value).build();
        //System.out.println("Sending to server: " + request);
        Kvservice.PutResponse response;
        try {
            response = blockingStub.put(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            Metadata metadata = e.getTrailers();
            String metadataValue = metadata.get(Metadata.Key.of("leaderPort", Metadata.ASCII_STRING_MARSHALLER));
            logger.log(Level.INFO, "New Leader port : " + metadataValue);
            if (metadataValue != null && metadataValue.length() > 0) throw new NoLongerLeaderException(Integer.parseInt(metadataValue));
            throw e;
        }
    }

    public int get(int key) throws Exception {
        Kvservice.GetRequest request = Kvservice.GetRequest.newBuilder().setKey(key).build();
        //System.out.println("Sending to server: " + request);
        Kvservice.GetResponse response;
        try {
            response = blockingStub.get(request);
            return response.getValue();
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            Metadata metadata = e.getTrailers();
            String metadataValue = metadata.get(Metadata.Key.of("leaderPort", Metadata.ASCII_STRING_MARSHALLER));
            if (metadataValue != null && metadataValue.length() > 0) throw new NoLongerLeaderException(Integer.parseInt(metadataValue));
            throw e;
        }
    }

    @SneakyThrows
    public Kvservice.RVResponse requestVote(Kvservice.RVRequest request) {
        Kvservice.RVResponse response = null;
        try {
            System.out.println(("Sending request vote: " + request));
            response = blockingStub.requestVoteRPC(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
        }

        return response;
    }

    @SneakyThrows
    public Kvservice.APEResponse appendEntries(Kvservice.APERequest request) {
//        Kvservice.Entry entry = Kvservice.Entry.newBuilder()
//                .setIndex(prevIndex + 1)
//                .setTerm(leaderTerm)
//                .setKey(key)
//                .setValue(value).build();
//        List<Kvservice.Entry> entries = new ArrayList<>();
//        entries.add(entry);
//
//        Kvservice.APERequest request = Kvservice.APERequest.newBuilder()
//                .setLeaderTerm(leaderTerm)
//                .setPrevLogIndex(prevIndex)
//                .setPrevLogTerm(prevTerm)
//                .addAllEntry(entries)
//                .build();

        Kvservice.APEResponse response = null;

        while (true) {
            try {
                System.out.println("Sending to server: " + request);
                response = blockingStub.appendEntriesRPC(request);
                break;
            } catch (StatusRuntimeException e) {
                if (request.getEntryList().size() == 0) break;
                logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
                Thread.sleep(1000);
            }
        }


        return response;
    }

    public Kvservice.APEResponse appendEntries(int leaderTerm, int prevIndex, int prevTerm, int key, int value) {
        Kvservice.Entry entry = Kvservice.Entry.newBuilder()
                .setIndex(prevIndex + 1)
                .setTerm(leaderTerm)
                .setKey(key)
                .setValue(value).build();
        List<Kvservice.Entry> entries = new ArrayList<>();
        entries.add(entry);

        Kvservice.APERequest request = Kvservice.APERequest.newBuilder()
                .setLeaderTerm(leaderTerm)
                .setPrevLogIndex(prevIndex)
                .setPrevLogTerm(prevTerm)
                .addAllEntry(entries)
                .build();

        System.out.println("Sending to server: " + request);
        Kvservice.APEResponse response = null;
        try {
            response = blockingStub.appendEntriesRPC(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
        }
        return response;
    }


    public static void main(String[] args) throws Exception {
        int key = 1;
        int val = 2;
        String serverAddress = "localhost:5051";
        ManagedChannel channel = ManagedChannelBuilder.forTarget(serverAddress)
                .usePlaintext()
                .build();
        try {
            KVSClient client = new KVSClient(channel);
//            client.put(key, val);
            client.appendEntries(0, -1, -1, 1, 100);
            client.appendEntries(1, 0, 0, 2, 200);
            client.appendEntries(0, 1, 0, 3, 300);
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
