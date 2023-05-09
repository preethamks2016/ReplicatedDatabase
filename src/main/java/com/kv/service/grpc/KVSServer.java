package com.kv.service.grpc;

import com.kv.store.KVStore;
import com.kv.store.KVStoreImpl;
import com.kv.store.LogStore;
import com.kv.store.LogStoreImpl;
import com.kvs.Kvservice;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.File;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import com.kvs.KVServiceGrpc;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;


public class KVSServer implements Watcher {
    private static final Logger logger = Logger.getLogger(KVSServer.class.getName());
    private LogStore logStore;
    private KVStore kvStore;
    private List<String> servers;
    private Server server;
    private ZooKeeper zk;
    private String znodePath;
    private String currentNodeId;
    private final String ZNODE_PREFIX = "/election-";
    private final String ZNODE_PATH = "/election";
    private final String LEADER_PATH = "/leader-data";
    private final int SESSION_TIMEOUT = 3000;
    private String IpPort = "";
    private long delay = 0;

    public KVSServer(String zkHostPort) throws IOException, InterruptedException, KeeperException {
        this.zk = new ZooKeeper(zkHostPort, SESSION_TIMEOUT, this);
//        if (zk.exists(ZNODE_PATH, false) == null) {
//            zk.create(ZNODE_PATH, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        }
    }

    private void electLeader(int port) throws KeeperException, InterruptedException, IOException {
        List<String> children = zk.getChildren(ZNODE_PATH, false);
        Collections.sort(children);
        String smallestChild = children.get(0);
        if (currentNodeId.equals(ZNODE_PATH + "/" + smallestChild)) {
            System.out.println("I am the leader!");
            // Do leader-specific work here

            // Update config
            zk.setData(LEADER_PATH, IpPort.getBytes(), -1);
            Thread.sleep(10000);

            // Update in memory servers
            ReadAllServers(port);
            KVServiceFactory.instantiateClasses(ServiceType.LEADER, logStore, servers, kvStore, port);
        } else {
            System.out.println("I am not the leader.");
            // Wait for the smallest node to change, then try to elect a leader again
            System.out.println("CurrentNodeId : " + currentNodeId.split("/")[2]);
            System.out.println("All Children");
            for (String c : children) {
                System.out.println("Child: " + c);
            }

            TreeSet<String> set = new TreeSet<String>(children);
            String prevNode = set.lower(currentNodeId.split("/")[2]);
            System.out.println("Watching on prevNode: " + prevNode);
            Stat prevNodeStat = zk.exists(ZNODE_PATH + "/" + prevNode, true);
        }
    }

    public void ReadAllServers(int port) throws InterruptedException, KeeperException {
        // Update in memory servers
        List<String> allServers = zk.getChildren(ZNODE_PATH, false);
        this.servers = new ArrayList<String>();
        for (String server : allServers) {
            String childPath = ZNODE_PATH + "/" + server;
            byte[] data = zk.getData(childPath, false, null);
            String dataStr = new String(data);
            if (dataStr.contains(Integer.toString(port))) continue;
            this.servers.add(dataStr);
        }
    }

    private void start(ServiceType serviceType, String IpPort) throws IOException, InterruptedException, KeeperException {
        this.IpPort = IpPort;
        int port = Integer.valueOf(IpPort.split(":")[1]);
        this.logStore = new LogStoreImpl("log" + port + ".txt", "meta" + port + ".txt");
        this.kvStore = new KVStoreImpl(port);

        currentNodeId = zk.create(ZNODE_PATH + ZNODE_PREFIX, IpPort.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Node created with id: " + currentNodeId);
        KVServiceFactory.instantiateClasses(serviceType, logStore, servers, kvStore, port);
        electLeader(port);
        //ReadAllServers(port);
        server = ServerBuilder.forPort(port).addService(new KVSImpl()).build().start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("Shutting down gRPC server");
                try {
                    System.out.println("Shutting down at - " + System.currentTimeMillis());
                    kvStore.stop();
                    server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                } catch (IOException e) {
                    e.printStackTrace(System.err);
                    throw new RuntimeException(e);
                }
            }
        });

        while (true) {
            try {
                System.out.println("The Server is now starting !");
                ScheduledExecutorService scheduledExecutor = KVServiceFactory.getInstance().start();
//                //wait for the scheduled executor to end
                scheduledExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                System.out.println("Executor is terminated");
                System.out.println("changing from - " + serviceType + " to - " + KVServiceFactory.getInstance().newServiceType);
                serviceType = KVServiceFactory.getInstance().newServiceType;
                //instantiate new class
                KVServiceFactory.instantiateClasses(serviceType, logStore, servers, kvStore, port);
            } catch (Exception e) {

            }
        }
    }


    @Override
    public void process(WatchedEvent watchedEvent) {
        try {
            System.out.println("Got watch event");
            electLeader(Integer.valueOf(IpPort.split(":")[1]));
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static class KVSImpl extends KVServiceGrpc.KVServiceImplBase {

        public static long delay = 0;

        public void put(Kvservice.PutRequest req, StreamObserver<Kvservice.PutResponse> responseObserver) {
            System.out.println("Got request from client: " + req);

            try {
                Kvservice.PutResponse response = KVServiceFactory.getInstance().put(req.getKey(), req.getValue());
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
            catch (StatusRuntimeException ex) {
                responseObserver.onError(ex);
            }
        }

        public void get(Kvservice.GetRequest req, StreamObserver<Kvservice.GetResponse> responseObserver)  {
            System.out.println("Got request from client: " + req);

            try {
                Kvservice.GetResponse response = KVServiceFactory.getInstance().get(req.getKey());
                try {
                    Thread.sleep(150);;
                }
                catch (Exception ex)
                {
                }
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
            catch (StatusRuntimeException ex) {
                responseObserver.onError(ex);
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        public void appendEntriesRPC(Kvservice.APERequest req, StreamObserver<Kvservice.APEResponse> responseObserver) {
            System.out.println("Got request from client: index:" + req.getIndex() + ", nEntries: " + req.getEntryList().size());
            try {
                Thread.sleep(delay);
                Kvservice.APEResponse reply = KVServiceFactory.getInstance().appendEntries(req);
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            }
            catch (Exception ex) {
                responseObserver.onError(Status.FAILED_PRECONDITION.withDescription("Failed check").asRuntimeException());
            }
        }

        public void requestVoteRPC(Kvservice.RVRequest req, StreamObserver<Kvservice.RVResponse> responseObserver) {
            System.out.println("Got request from client: " + req);
            try {
                Kvservice.RVResponse reply = KVServiceFactory.getInstance().requestVotes(req);
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            }
            catch (Exception ex) {
                responseObserver.onError(Status.FAILED_PRECONDITION.withDescription("Failed check").asRuntimeException());
            }
        }


    }

    static class KVServiceFactory {

        static KVService kvService;

        public static KVService getInstance() {
            return kvService;
        }

        public static void instantiateClasses(ServiceType type, LogStore logStore, List<String> servers, KVStore kvStore, int port) throws IOException {
            switch (type){
                case LEADER:
                    kvService = new LeaderKVSService(logStore, servers, kvStore, port);
                    break;
                case FOLLOWER:
                    kvService = new FollowerKVSService(logStore, kvStore, port);
                    break;
//                case CANDIDATE:
//                    kvService = new CandidateKVSService(logStore, servers, kvStore, port);
//                    break;
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        final KVSServer kvs = new KVSServer("10.10.1.2:2181");
        kvs.start(ServiceType.FOLLOWER, args[0]);
        if (args.length >= 2) {
            System.out.println("Putting delay of " + args[1]);
            KVSImpl.delay = Long.parseLong(args[1]);
        }
        kvs.server.awaitTermination();
    }

}
