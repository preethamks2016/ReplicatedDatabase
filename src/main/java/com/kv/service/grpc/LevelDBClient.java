package com.kv.service.grpc;

import com.google.common.util.concurrent.ListenableFuture;
import com.kv.service.grpc.exception.NoLongerLeaderException;
import com.kvs.KVServiceGrpc;
import com.kvs.Kvservice;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.Thread.sleep;

public class LevelDBClient implements Watcher {

    private ZooKeeper zooKeeper;
    private String leaderPath;

    private String servicePath;

    private int latestIndex = -1;

    AddressNameResolverFactory nameResolverFactory;

    String filePath = "readMetrics.txt";


    private KVServiceGrpc.KVServiceStub  writeStub;

    private KVServiceGrpc.KVServiceStub readStub;

    private ConsistencyType consistencyType;

    private static final Logger logger = Logger.getLogger(LevelDBClient.class.getName());

    private Object indexObject;

    BufferedWriter writer;

    List<Long> readLatencies = new CopyOnWriteArrayList<>();

    List<Long> writeLatencies = new CopyOnWriteArrayList<>();

    private Object countObj;

    int count = 0;




    public LevelDBClient(String zooKeeperAddress, String leaderPath, String servicePath, ConsistencyType consistencyType) throws Exception {
        System.out.println("Initializing client");
        this.zooKeeper = new ZooKeeper(zooKeeperAddress, 2000, this);
        this.leaderPath = leaderPath;
        this.servicePath = servicePath;
        this.consistencyType = consistencyType;
        indexObject = new Object();
        countObj = new Object();
        populateWriteServers();
        populateReadServers();

        writer = new BufferedWriter(new FileWriter(filePath));
        writer.write("Hello!");
    }

    public LevelDBClient() {

    }


    private void populateReadServers() throws InterruptedException, KeeperException, FileNotFoundException {
        Stat stat = zooKeeper.exists(servicePath, this);
        if (stat == null) {
            throw new RuntimeException("Servers data does not exist");
        }
        List<SocketAddress> serverAddresses = getServerAddresses();

        if (nameResolverFactory == null) {
            nameResolverFactory = new AddressNameResolverFactory(
                    serverAddresses
            );
            ManagedChannel readChannel = ManagedChannelBuilder.forTarget("levelDb")
                    .nameResolverFactory(nameResolverFactory)
                    .defaultLoadBalancingPolicy("round_robin")
                    .usePlaintext().build();

            readStub = com.kvs.KVServiceGrpc.newStub(readChannel);
            //newBlockingStub(readChannel);
        } else {
            nameResolverFactory.refresh(serverAddresses);
        }
    }

    private List<SocketAddress> getServerAddresses() throws InterruptedException, KeeperException {
        List<String> children = zooKeeper.getChildren(servicePath, true);
        List<String> serverAddresses = new ArrayList<>();
        for (String child : children) {
            byte[] data = zooKeeper.getData(servicePath + "/" + child, false, null);
            String nodeInfo = new String(data);
            serverAddresses.add(nodeInfo);
        }
        List<SocketAddress> serverDetails = new ArrayList<>();
        // Notify the listener with the updated server list

        for (String server : serverAddresses) {
            System.out.println("Server address - " + server);
            SocketAddress address1 = new InetSocketAddress(server.split(":")[0], Integer.valueOf(server.split(":")[1]));
            serverDetails.add(address1);

        }
        return serverDetails;
    }

    @SneakyThrows
    @Override
    public void process(WatchedEvent event) {
        if (event.getPath().equals(leaderPath)) {
            if (event.getType() == Event.EventType.NodeDataChanged) {
                System.out.println("Leader changed. Updating the leader config");
                populateWriteServers();
            }
        } else if (event.getPath().equals(servicePath)) {
            System.out.println("Received watch for change in the server list");
            //update the list of servers
            populateReadServers();
        }
    }

    private void populateWriteServers() throws Exception {
        String leader = getLeader();
        //System.out.println("Leader details - " + leader);
        ManagedChannel writeChannel = ManagedChannelBuilder.forTarget(leader)
                .usePlaintext()
                .build();
        //TODO :: synchronization necessary?
        synchronized (this) {
            writeStub = com.kvs.KVServiceGrpc.newStub(writeChannel);
        }
    }


    public String getLeader() throws Exception {
        Stat stat = zooKeeper.exists(leaderPath, this);
        if (stat == null) {
            throw new RuntimeException("Leader node does not exist!");
        }
        byte[] data = zooKeeper.getData(leaderPath, false, null);
        return new String(data);

    }

    public void put(int key, int value) throws NoLongerLeaderException {
        Long starttime = System.currentTimeMillis();
        com.kvs.Kvservice.PutRequest request = com.kvs.Kvservice.PutRequest.newBuilder().setKey(key).setValue(value).build();
        //System.out.println("Sending to server: " + request);
        CompletableFuture<Kvservice.PutResponse> asyncResponse = triggerAsyncWriteRequest(request);
        //System.out.println("completed put request");
        asyncResponse.thenAccept(response -> {
            //System.out.println("completed put request");
            Long timeTaken  = System.currentTimeMillis() - starttime;
            //System.out.println("Time taken - " + (timeTaken));
            writeLatencies.add(timeTaken);
            updateIndex(response.getIndex());
            synchronized (countObj) {
                count++;
            }
        });
    }

    private void updateIndex(int index) {
        synchronized (indexObject) {
            if(index > latestIndex) {
                latestIndex = index;
            }
        }
    }

    public void get(int key) throws Exception {
        Long startTime = System.currentTimeMillis();
        com.kvs.Kvservice.GetRequest request = com.kvs.Kvservice.GetRequest.newBuilder().setKey(key).build();
        CompletableFuture<Kvservice.GetResponse> responseFuture;
        int index = latestIndex;
        if(consistencyType.equals(ConsistencyType.STRONG)) {
            responseFuture = triggerAsyncGetRequest(request, writeStub);
        } else {
            responseFuture = triggerAsyncGetRequest(request, readStub);
        }

        responseFuture.thenAccept(response -> {
            if(index > response.getIndex()) {
                CompletableFuture<Kvservice.GetResponse> leaderRead = triggerAsyncGetRequest(request, writeStub);
                leaderRead.thenAccept(leaderReadResponse -> {
                    System.out.println("Got the right response");
                });
            }
            Long timetaken = System.currentTimeMillis() - startTime;
            //System.out.println("Time taken = " + timetaken);
            readLatencies.add(timetaken);
            updateIndex(response.getIndex());
            synchronized (countObj) {
                count++;
            }
            // Handle the response
        }).exceptionally(ex -> {
            // Handle exceptions
            return null;
        });
    }

    private CompletableFuture<Kvservice.PutResponse> triggerAsyncWriteRequest(Kvservice.PutRequest request) {

        CompletableFuture<Kvservice.PutResponse> future = new CompletableFuture<>();

        writeStub.put(request, new StreamObserver<Kvservice.PutResponse>() {
            @Override
            public void onNext(Kvservice.PutResponse response) {
                future.complete(response);  // Set the response on the CompletableFuture
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);  // Set the exception on the CompletableFuture
            }

            @Override
            public void onCompleted() {
                // Handle completion
            }
        });

        return future;
    }

    private CompletableFuture<Kvservice.GetResponse> triggerAsyncGetRequest(Kvservice.GetRequest request, KVServiceGrpc.KVServiceStub stub) {

        CompletableFuture<Kvservice.GetResponse> future = new CompletableFuture<>();

        stub.get(request, new StreamObserver<Kvservice.GetResponse>() {
            @Override
            public void onNext(Kvservice.GetResponse response) {
                future.complete(response);  // Set the response on the CompletableFuture
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);  // Set the exception on the CompletableFuture
            }

            @Override
            public void onCompleted() {
                // Handle completion
            }
        });

        return future;
    }

    public static void main(String[] args) throws Exception {
        LevelDBClient client = new LevelDBClient("10.10.1.2:2181", "/leader-data", "/election", ConsistencyType.EVENTUAL);
//        client.put(1, 2);
//        client.put(2, 2);
//        sleep(5000);
//
//        client.get(1);
//        client.get(2);
//        sleep(5000);

        //System.out.println(client.get(1));
        Long startTime = System.currentTimeMillis();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CompletionService<?> completionService = new ExecutorCompletionService<>(executor);
        int numRequests = Integer.valueOf(args[0]);
        for (int i = 0 ; i<numRequests; i++) {
            int finalI = i;
            int finalI1 = i;
            int finalI2 = i;
            completionService.submit(() -> {
                Kvservice.GetResponse res =  null;
                int x = finalI1;
                //client.put(x, x * 2);
                client.put(x, x*2);

                return null;
            });
        }

//        int count = 0;
//        while (count < numRequests) {
//            completionService.take().get();
//            count++;
//        }
        while(client.count < numRequests) {
            Thread.sleep(1000);
        }
        Long timeMillis = System.currentTimeMillis() - startTime;

        System.out.println("Time taken - " +  timeMillis);

        FileWriter readwriter = new FileWriter("readLatencies.txt");
        FileWriter writewriter = new FileWriter("writeLatencies.txt");

        for (Long latency : client.readLatencies) {
            readwriter.write(String.valueOf(latency) +  System.lineSeparator());
        }
        for (Long latency : client.writeLatencies) {
            //System.out.println("writing");
            writewriter.write(String.valueOf(latency) + System.lineSeparator());
        }

        readwriter.close();
        writewriter.close();

        System.out.println("Done writing");
//        while (true){
//
//            sleep(3000);
//        }

    }
}
