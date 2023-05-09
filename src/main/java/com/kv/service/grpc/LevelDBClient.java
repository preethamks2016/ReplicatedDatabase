package com.kv.service.grpc;

import com.google.gson.stream.JsonReader;
import com.kv.service.grpc.exception.NoLongerLeaderException;
import io.grpc.*;
import lombok.SneakyThrows;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.google.gson.Gson;

import static io.netty.util.CharsetUtil.UTF_8;
import static java.lang.Thread.sleep;

public class LevelDBClient implements Watcher {

    private ZooKeeper zooKeeper;
    private String leaderPath;

    private String servicePath;

    boolean blockWrites = true;

    private int latestIndex = -1;

    AddressNameResolverFactory nameResolverFactory;


    private com.kvs.KVServiceGrpc.KVServiceBlockingStub writeStub;

    private com.kvs.KVServiceGrpc.KVServiceBlockingStub readStub;

    private ConsistencyType consistencyType;

    private static final Logger logger = Logger.getLogger(LevelDBClient.class.getName());

    private Object indexObject;


    public LevelDBClient(String zooKeeperAddress, String leaderPath, String servicePath,  ConsistencyType consistencyType) throws Exception {
        System.out.println("Initializing client");
        this.zooKeeper = new ZooKeeper(zooKeeperAddress, 2000, this);
        this.leaderPath = leaderPath;
        this.servicePath = servicePath;
        this.consistencyType = consistencyType;
        indexObject = new Object();
        populateWriteServers();
        populateReadServers();

    }

    public LevelDBClient() {

    }


    private void populateReadServers() throws InterruptedException, KeeperException, FileNotFoundException {
        Stat stat = zooKeeper.exists(servicePath, this);
        if (stat == null) {
            throw new RuntimeException("Servers data does not exist");
        }
        List<SocketAddress> serverAddresses = getServerAddresses();

        if(nameResolverFactory == null) {
            nameResolverFactory = new AddressNameResolverFactory(
                    serverAddresses
            );
            ManagedChannel readChannel = ManagedChannelBuilder.forTarget("levelDb")
                    .nameResolverFactory(nameResolverFactory)
                    .defaultLoadBalancingPolicy("round_robin")
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
        com.kvs.Kvservice.PutRequest request = com.kvs.Kvservice.PutRequest.newBuilder().setKey(key).setValue(value).build();
        //System.out.println("Sending to server: " + request);
        com.kvs.Kvservice.PutResponse response;
        try {
            response = writeStub.put(request);
            synchronized (indexObject) {
                if(response.getIndex() > latestIndex) {
                    latestIndex = response.getIndex();
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
    }

    public int get(int key) throws Exception {
        com.kvs.Kvservice.GetRequest request = com.kvs.Kvservice.GetRequest.newBuilder().setKey(key).build();
        //System.out.println("Sending to server: " + request);
        com.kvs.Kvservice.GetResponse response = null;
        try {
            if(consistencyType.equals(ConsistencyType.STRONG)){
                //TODO :: get leader config and read
            } else {
                response = readStub.get(request);
            }
            if(consistencyType.equals(ConsistencyType.MONOTONIC_READS) && response.getIndex() < latestIndex) {
                response = writeStub.get(request);
            }
            synchronized (indexObject) {
                if(response.getIndex() > latestIndex) {
                    latestIndex = response.getIndex();
                }
            }
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
        LevelDBClient client = new LevelDBClient("10.10.1.2:2181", "/leader-data", "/election", ConsistencyType.EVENTUAL);
//        for (int i = 0; i < 5000; i++) {
//            System.out.println(i);
//            client.put(i, 2*i);
//        }

        for (int i = 0; i < 5; i++) {
            System.out.println(client.get(2342));
        }

//        System.out.println(client.get(1));
//
//        while (true){
//
//            sleep(3000);
//        }

    }

}
