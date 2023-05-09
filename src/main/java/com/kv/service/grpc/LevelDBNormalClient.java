package com.kv.service.grpc;

import com.google.gson.stream.JsonReader;
import com.kv.service.grpc.exception.NoLongerLeaderException;
import com.kvs.Kvservice;
import io.grpc.*;
import lombok.SneakyThrows;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.google.gson.Gson;

import static io.netty.util.CharsetUtil.UTF_8;
import static java.lang.Thread.sleep;

public class LevelDBNormalClient implements Watcher {

    private ZooKeeper zooKeeper;
    private String leaderPath;

    private String servicePath;

    boolean blockWrites = true;

    private int latestIndex = -1;

    private List<Long> readLatencies = new CopyOnWriteArrayList<>();

    private List<Long> writeLatencies = new CopyOnWriteArrayList<>();

    AddressNameResolverFactory nameResolverFactory;


    private com.kvs.KVServiceGrpc.KVServiceBlockingStub writeStub;

    private com.kvs.KVServiceGrpc.KVServiceBlockingStub readStub;

    private ConsistencyType consistencyType;

    private static final Logger logger = Logger.getLogger(LevelDBClient.class.getName());

    private Object indexObject;

    private Object monotonicRetryCount;

    int monotonicCount = 0;

    String readLoadBalancePolicy;

    int numServers = -1;


    public LevelDBNormalClient(String zooKeeperAddress, String leaderPath, String servicePath,  ConsistencyType consistencyType, String readLoadBalancePolicy) throws Exception {
        System.out.println("Initializing client");
        this.zooKeeper = new ZooKeeper(zooKeeperAddress, 2000, this);
        this.leaderPath = leaderPath;
        this.servicePath = servicePath;
        this.consistencyType = consistencyType;
        indexObject = new Object();
        monotonicRetryCount = new Object();
        this.readLoadBalancePolicy = readLoadBalancePolicy;
        populateWriteServers();
        populateReadServers();

    }


    private void populateReadServers() throws InterruptedException, KeeperException, FileNotFoundException {
        Stat stat = zooKeeper.exists(servicePath, this);
        if (stat == null) {
            throw new RuntimeException("Servers data does not exist");
        }
        List<SocketAddress> serverAddresses = getServerAddresses();
        if(numServers == -1) {
            numServers = serverAddresses.size();
        }

        if(nameResolverFactory == null) {
            nameResolverFactory = new AddressNameResolverFactory(
                    serverAddresses
            );
            ManagedChannel readChannel = ManagedChannelBuilder.forTarget("levelDb")
                    .nameResolverFactory(nameResolverFactory)
                    .defaultLoadBalancingPolicy(readLoadBalancePolicy)
                    .usePlaintext().build();

            readStub = com.kvs.KVServiceGrpc.newBlockingStub(readChannel);
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

        for(String server : serverAddresses) {
            System.out.println("Server address - " + server);
            SocketAddress address1 = new InetSocketAddress(server.split(":")[0], Integer.valueOf(server.split(":")[1]));
            serverDetails.add(address1);

        }
        return serverDetails;
    }

    @SneakyThrows
    @Override
    public void process(WatchedEvent event) {
        System.out.println("Even for path - " + event.getPath());
        if(event.getPath().equals(leaderPath)) {
            if(event.getType() == Event.EventType.NodeDataChanged) {
                System.out.println("Leader changed. Updating the leader config");
                populateWriteServers();
            }
        } else if(event.getPath().equals(servicePath)) {
            System.out.println("Received watch for change in the server list");
            //update the list of servers
            populateReadServers();
        }
    }

    private void populateWriteServers() throws Exception {
        String leader = getLeader();
        System.out.println("Leader details - " + leader);
        ManagedChannel writeChannel = ManagedChannelBuilder.forTarget(leader)
                .usePlaintext()
                .build();
        //TODO :: synchronization necessary?
        synchronized (this) {
            writeStub = com.kvs.KVServiceGrpc.newBlockingStub(writeChannel);
        }
    }


    public String getLeader() throws Exception {
        Stat stat = zooKeeper.exists(leaderPath, this);
        if (stat == null) {
            throw new RuntimeException("Leader node does not exist!");
        }
        byte[] data = zooKeeper.getData(leaderPath, false, null);
        return  new String(data);

    }

    public void put(int key, int value) throws NoLongerLeaderException {
        Long startTime = System.currentTimeMillis();
        com.kvs.Kvservice.PutRequest request = com.kvs.Kvservice.PutRequest.newBuilder().setKey(key).setValue(value).build();
        //System.out.println("Sending to server: " + request);
        Kvservice.PutResponse response;
        try {
            response = writeStub.put(request);
            // for read my writes update the index so the client tries to read all the recent writes
            if (consistencyType.equals(ConsistencyType.READ_MY_WRITES)) {
                synchronized (indexObject) {
                    if (response.getIndex() > latestIndex) {
                        latestIndex = response.getIndex();
                    }
                }
            }
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            Metadata metadata = e.getTrailers();
            String metadataValue = metadata.get(Metadata.Key.of("leaderPort", Metadata.ASCII_STRING_MARSHALLER));
            logger.log(Level.INFO, "New Leader port : " + metadataValue);
            if (metadataValue != null && metadataValue.length() > 0) throw new NoLongerLeaderException(Integer.parseInt(metadataValue));
            throw e;
        }
        Long timeMillis = System.currentTimeMillis() - startTime;
        writeLatencies.add(timeMillis);

    }

    public int get(int key) throws Exception {
        int index = latestIndex;
        Long startTime = System.currentTimeMillis();
        com.kvs.Kvservice.GetRequest request = com.kvs.Kvservice.GetRequest.newBuilder().setKey(key).build();
        //System.out.println("Sending to server: " + request);
        com.kvs.Kvservice.GetResponse response = null;
        try {
            if(consistencyType.equals(ConsistencyType.STRONG)) {
                response = writeStub.get(request);
            } else {
                response = readStub.get(request);
            }
            if((consistencyType.equals(ConsistencyType.MONOTONIC_READS) || consistencyType.equals(ConsistencyType.READ_MY_WRITES)) && response.getIndex() < index) {
                synchronized (monotonicRetryCount){
                    monotonicCount++;
                }
                response = writeStub.get(request);
            }
            synchronized (indexObject) {
                if(response.getIndex() > latestIndex) {
                    latestIndex = response.getIndex();
                }
            }
            Long timetaken = System.currentTimeMillis() - startTime;
            readLatencies.add(timetaken);
            return response.getValue();
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            Metadata metadata = e.getTrailers();
            String metadataValue = metadata.get(Metadata.Key.of("leaderPort", Metadata.ASCII_STRING_MARSHALLER));
            if (metadataValue != null && metadataValue.length() > 0) throw new NoLongerLeaderException(Integer.parseInt(metadataValue));
            throw e;
        }



    }

    public static void main(String[] args) throws Exception {
        LevelDBNormalClient client = new LevelDBNormalClient("10.10.1.2:2181", "/leader-data", "/election", ConsistencyType.valueOf(args[1]), args[3]);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        CompletionService<Kvservice.GetResponse> completionService = new ExecutorCompletionService<Kvservice.GetResponse>(executor);

        int numRequests = Integer.valueOf(args[0]);
        String req = args[2];
        long beginning = System.currentTimeMillis();
        if(req.equals("get")) {
            for (int i = 0; i < numRequests; i++) {
                int finalI = i;
                completionService.submit(() -> {
                    Kvservice.GetResponse res = null;
                    //client.put(finalI, finalI *2);
                    client.get(finalI);
                    //System.out.println("put request completed for i = " + finalI);
                    return res;
                });
            }
        } else if (req.equals("put")) {
            for (int i = 0; i < numRequests; i++) {
                int finalI = i;
                completionService.submit(() -> {
                    Kvservice.GetResponse res = null;
                    //client.put(finalI, finalI *2);
                    client.put(finalI, finalI * 2);
                    //System.out.println("put request completed for i = " + finalI);
                    return res;
                });
            }
        } else {

            for (int i = 0; i < numRequests; i++) {
                int finalI = i;
                completionService.submit(() -> {
                    Kvservice.GetResponse res = null;
                    //client.put(finalI, finalI *2);
                    client.put(finalI, finalI * 3);
                    for(int k =0; k< 10; k++) {
                        int val = client.get(finalI);
                    }
                    //int val = client.get(finalI);
                    //System.out.println("put request completed for i = " + finalI);
                    return res;
                });
//                for(int k =0; k< 10; k++) {
//                    completionService.submit(() -> {
//                        Kvservice.GetResponse res = null;
//                        //client.put(finalI, finalI *2);
//                        int val = client.get(finalI);
//
//                        //System.out.println("Value of " + finalI + " == " + val);
//                        //System.out.println("put request completed for i = " + finalI);
//                        return res;
//                    });
//                }

            }
        }
        int count = 0;
        while (count < numRequests - 1) {
            completionService.take().get();
            count++;
        }

        long timeMillis = System.currentTimeMillis() - beginning;

        System.out.println("Time taken - " +  timeMillis);
        System.out.println("Monotonic count - " +  client.monotonicCount);


        String fileName = client.numServers + "_" + args[1] + "_" + args[0] + "_" + args[3] + "_" + req;
        FileWriter readwriter = new FileWriter("readLatencies_" + fileName + ".txt");
        FileWriter writewriter = new FileWriter("writeLatencies_" + fileName +  ".txt");
        FileWriter overallMetrics = new FileWriter("overallMetrics_" + fileName +  ".txt");


        for (Long latency : client.readLatencies) {
            readwriter.write(String.valueOf(latency) +  System.lineSeparator());
        }
        for (Long latency : client.writeLatencies) {
            writewriter.write(String.valueOf(latency) + System.lineSeparator());
        }

        overallMetrics.write(String.format("Total time - %d\n", timeMillis));
        overallMetrics.write(String.format("Monotonic Count - %d\n", client.monotonicCount));

        readwriter.close();
        writewriter.close();
        overallMetrics.close();

        System.exit(0);
        return;

//        System.out.println(client.get(1));
//
//        while (true){
//
//            sleep(3000);
//        }

    }

}