package com.kv.service.grpc;

import com.kv.service.grpc.exception.NoLongerLeaderException;
import io.grpc.*;
import lombok.SneakyThrows;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

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


    public LevelDBClient(String zooKeeperAddress, String leaderPath, String servicePath,  ConsistencyType consistencyType) throws Exception {
        this.zooKeeper = new ZooKeeper(zooKeeperAddress, 5000, this);
        this.leaderPath = leaderPath;
        this.servicePath = servicePath;
        this.consistencyType = consistencyType;
        populateWriteServers();
        populateReadServers();

    }

    public LevelDBClient() {

    }


    private void populateReadServers() throws InterruptedException, KeeperException {
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
                    .usePlaintext()
                    .build();
            readStub = com.kvs.KVServiceGrpc.newBlockingStub(readChannel);
        } else {
            nameResolverFactory.refresh(serverAddresses);
        }
    }

    private List<SocketAddress> getServerAddresses() throws InterruptedException, KeeperException {
        List<String> children = zooKeeper.getChildren(servicePath, false);
        List<String> serverAddresses = new ArrayList<>();
        for (String child : children) {
            byte[] data = zooKeeper.getData(leaderPath + "/" + child, false, null);
            String nodeInfo = new String(data);
            serverAddresses.add(nodeInfo);
        }
        List<SocketAddress> serverDetails = new ArrayList<>();
        // Notify the listener with the updated server list

        for(String server : serverAddresses) {
            SocketAddress address1 = new InetSocketAddress(server.split(":")[0], Integer.valueOf(server.split(":")[1]));
            serverDetails.add(address1);

        }
        return serverDetails;
    }

    @SneakyThrows
    @Override
    public void process(WatchedEvent event) {
        if(event.getPath().equals(leaderPath)) {
            if(event.getType() == Event.EventType.NodeDeleted){
                blockWrites = true;
            } else {
                populateWriteServers();
            }
        } else if(event.getPath().equals(servicePath)) {
            //update the list of servers
            populateReadServers();
        }
    }

    private void populateWriteServers() throws Exception {
        String leader = getLeader();
        ManagedChannel writeChannel = ManagedChannelBuilder.forTarget(leader)
                .usePlaintext()
                .build();
        writeStub = com.kvs.KVServiceGrpc.newBlockingStub(writeChannel);
        blockWrites = false;
    }


    public String getLeader() throws Exception {
        Stat stat = zooKeeper.exists(leaderPath, this);
        if (stat == null) {
            throw new RuntimeException("Leader node does not exist!");
        }
        byte[] data = zooKeeper.getData(leaderPath, false, null);
        String serverAddress = new String(data);
        return serverAddress;

    }

    public void put(int key, int value) throws NoLongerLeaderException {
        com.kvs.Kvservice.PutRequest request = com.kvs.Kvservice.PutRequest.newBuilder().setKey(key).setValue(value).build();
        //System.out.println("Sending to server: " + request);
        com.kvs.Kvservice.PutResponse response;
        try {
            response = writeStub.put(request);
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
                //TODO :: get leader config and talk to leader
                response = writeStub.get(request);
            }

            latestIndex = response.getIndex();

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
        LevelDBClient client = new LevelDBClient();
        ZooKeeper zk = new ZooKeeper("c220g1-031128.wisc.cloudlab.us:2181", 1000,client );
        System.out.println("Ran");

    }

}
