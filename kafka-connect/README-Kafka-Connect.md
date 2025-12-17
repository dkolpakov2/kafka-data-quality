1. Directory Structure
kafka-connect/
├── Dockerfile
├── plugins/
│   ├── cassandra-source/
│   │   └── cassandra-source-connector.jar
│   └── hash-smt/
│       └── hash-row-smt.jar
├── connect-distributed.properties
└── README.md

2. Build 
docker-compose build kafka-connect
3. Run
docker-compose up -d kafka-connect

4. Verify
curl http://localhost:8083/connectors

5. Deploy Cassandra → Topic-2 Connector
 curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @cassandra-cloud-source.json

6. Verify
  curl http://localhost:8083/connectors/cassandra-cloud-hash-source/statushi 
  
 7. Build Jsr
 mvn clean package

 8. Install into Kafka Connect
    cp target/hash-row-smt-1.0.0.jar kafka-connect/plugins/hash-smt/
 9. Restart Kafka Connect.
   docker-compose down -v kafka-connect
   docker-compose build --no-cache kafka-connect
	 docker-compose up -d kafka-connect
  
===========================================
### 777 Next Steps ###
Generate Hash SMT Maven project (full code)
Add Schema Registry
Add exactly-once guarantees
Add TLS + SASL
Add DataDog monitoring for Connect
Generate Helm chart for Connect

After SMT Java
Add Schema-aware (Avro) support
Add salting / versioned hashing
Add HMAC for tamper resistance
Add unit tests
Add multi-column PK hashing
Add Cassandra row canonicalization
Add DataDog metrics inside SMT
===========================================  