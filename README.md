# ReplicatedDatabase
ReplicatedDatabase using Raft protocol 

Build :
mvn clean install

Run server :
java -cp ./target/ReplicatedDatabase-1.0-SNAPSHOT.jar com.kv.service.grpc.KVSServer FOLLOWER <port>

Trigger concurrent requests 
java -cp ./target/ReplicatedDatabase-1.0-SNAPSHOT.jar com.kv.store.MultipleRequests 200 <leader-ip> <port>
