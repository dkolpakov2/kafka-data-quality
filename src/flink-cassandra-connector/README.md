# Flink Cassandra Connector

This project provides a connector for integrating Apache Flink with Apache Cassandra. It allows users to read from and write to Cassandra databases using Flink's data stream processing capabilities.

## Overview

The Flink Cassandra Connector is designed to facilitate seamless interaction between Flink applications and Cassandra. It includes features for configuring the connector, executing queries, and managing data streams.
## Docker

## Deploy JAR Job in local docker:
3. Copy the JAR File to the Flink JobManager
# Copy the JAR file to the running flink_jobmanager container:
docker cp target/FlinkCassandraConnector-1.0-SNAPSHOT.jar flink_jobmanager:/opt/flink/lib/

4. Submit the Job to Flink
#  Access the Flink JobManager container:
docker exec -it. flink_jobmanager bash
## Submit the job using the Flink CLI:
./bin/flink run /opt/flink/lib/FlinkCassandraConnector-1.0-SNAPSHOT.jar
##
5. Verify the Job
# Open the Flink Web UI at http://localhost:8081 to monitor the job.
# Check the logs in the Flink JobManager and TaskManager containers for any output:

docker logs flink_jobmanager
docker logs flink_taskmanager
=============================
## Fix 2. Check reference.conf for akka 
The reference.conf file is used to configure Akka. Ensure that the akka.stream.materializer property is defined in your reference.conf file. If it’s missing, add the following:
akka.stream.materializer {
  initial-input-buffer-size = 4
  max-input-buffer-size = 16
}
## 6. Debugging Tips
If the issue persists, check the reference.conf file inside the JAR
>> jar tf flink-cassandra-connector.jar | grep reference.conf
## Extract and inspect the file
jar xf flink-cassandra-connector.jar reference.conf


## Error when run jar in docker :
NoClassDefFoundError: org/apache/flink/api/common/Program at org.apache.flink.client.program.PackageProgram.<init>(PackageProgram.java:202)

=================================
## Project Structure

```
flink-cassandra-connector
├── src
│   ├── main
│   │   ├── java
│   │   │   └── com
│   │   │       └── example
│   │   │           └── flinkcassandra
│   │   │               ├── FlinkCassandraConnector.java
│   │   │               └── utils
│   │   │                   └── CassandraUtils.java
│   │   └── resources
│   │       └── application.properties
│   ├── test
│       ├── java
│       │   └── com
│       │       └── example
│       │           └── flinkcassandra
│       │               └── FlinkCassandraConnectorTest.java
│       └── resources
├── pom.xml
└── README.md
```

## Setup Instructions

1. **Clone the repository:**
   ```
   git clone <repository-url>
   cd flink-cassandra-connector
   ```

2. **Build the project:**
   ```
   mvn clean install
   ```

3. **Configure the connector:**
   Update the `src/main/resources/application.properties` file with your Cassandra connection details.

4. **Run your Flink application:**
   Use the `FlinkCassandraConnector` class to integrate with your Flink job.

## Usage Example

```java
// Example of using FlinkCassandraConnector
FlinkCassandraConnector connector = new FlinkCassandraConnector();
connector.configure();
connector.executeQuery("SELECT * FROM your_table");
```

## Testing

Unit tests for the `FlinkCassandraConnector` are located in the `src/test/java/com/example/flinkcassandra/FlinkCassandraConnectorTest.java` file. Run the tests using:

```
mvn test
```

## License

This project is licensed under the MIT License. See the LICENSE file for more details.
##
I need PK name and value, and "keyspace", , "table" get from Kafka message to pass all 4 values to queryCassandra

##
create message to send to KafkaProducer topic "reconsile" with fields: {"pk":"value", "hash": "hashed input-kafka message", "cass_hash": "hased cassandra row", "payload": "Cassandra row", "ts": timestemp, "action": INSERT/DELETE/UPDATE }

## ===============================================
Kafka → Flink DataStream → CassandraSink → CqlSession (internal)

## 1. Cassandra Sink Builder:
CassandraSink.addSink(stream)
    .setClusterBuilder(new CassandraClusterBuilder())
    .setQuery("INSERT INTO ks.table (id, value) VALUES (?, ?);")
    .build();

## 4. Multiple Cassandra nodes
@Override
protected CqlSessionBuilder buildSession(CqlSessionBuilder builder) {
    return builder
        .addContactPoint(new InetSocketAddress("node1", 9042))
        .addContactPoint(new InetSocketAddress("node2", 9042))
        .withLocalDatacenter("dc1");
// If Authentication needed:
        .withAuthCredentials("username", "password");

}

## 6. SSL / advanced driver options
If you need full driver control, you can load a driver config:

DriverConfigLoader loader =
    DriverConfigLoader.fromClasspath("application.conf");

return builder
    .withConfigLoader(loader);

## Example application.conf:
datastax-java-driver {
  basic.contact-points = [ "host1:9042", "host2:9042" ]
  basic.load-balancing-policy.local-datacenter = dc1
  advanced.connection {
    connect-timeout = 5s
    init-query-timeout = 5s
  }
}

##### Integration ########################################
Kafka(input-topic)
   ↓
Flink Source
   ↓
ProcessFunction (query Cassandra + hash)
   ↓
Kafka Sink (reconcile)
## ------------------------------
Flink DataStream API
Kafka messages are JSON
Cassandra queried by primary key
Hash = SHA-256
Output is JSON to Kafka topic reconcile

----------------------------------------------------------
## Input message
{
  "keyspace": "ks1",
  "tableName": "users",
  "pk": "123",
  "payload": {
    "name": "alice",
    "age": 30
  }
}

-------------------------
### 2 mvn 
<dependencies>
    <!-- Flink -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-streaming-java</artifactId>
        <version>1.17.1</version>
    </dependency>

    <!-- Kafka -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-kafka</artifactId>
        <version>1.17.1</version>
    </dependency>

    <!-- Cassandra -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-cassandra</artifactId>
        <version>1.17.1</version>
    </dependency>

    <!-- JSON -->
    <dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-databind</artifactId>
        <version>2.15.2</version>
    </dependency>
</dependencies>

## 3 Java Pojo input message:
public class InputMessage {
    public String keyspace;
    public String tableName;
    public String pk;
    public JsonNode payload;
}

## 4 JAva Pojo Output message:
public class ReconcileMessage {
    public String pk;
    public String hashKafkaMessage;
    public String hashCassandraRow;
    public JsonNode payload;
}

## 5 Cassandra session BUilder:
public class CassandraClusterBuilder extends ClusterBuilder {

    @Override
    protected CqlSessionBuilder buildSession(CqlSessionBuilder builder) {
        return builder
            .addContactPoint(new InetSocketAddress("cassandra-host", 9042))
            .withLocalDatacenter("dc1");
    }
}

## 6. Core Flink job
public class KafkaCassandraReconcileJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka source
        KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers("kafka:9092")
                        .setTopics("input-topic")
                        .setGroupId("reconcile-group")
                        .setValueOnlyDeserializer(
                                new SimpleStringSchema())
                        .build();

        DataStream<String> rawStream =
                env.fromSource(source,
                        WatermarkStrategy.noWatermarks(),
                        "kafka-source");

        DataStream<ReconcileMessage> reconciled =
                rawStream.process(new ReconcileProcessFunction());

        // Kafka sink
        KafkaSink<String> sink =
                KafkaSink.<String>builder()
                        .setBootstrapServers("kafka:9092")
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic("reconcile")
                                        .setValueSerializationSchema(
                                                new SimpleStringSchema())
                                        .build())
                        .build();

        reconciled
            .map(msg -> new ObjectMapper().writeValueAsString(msg))
            .sinkTo(sink);

        env.execute("Kafka Cassandra Reconcile Job");
    }
}


## 7. 7. ProcessFunction (Cassandra lookup + hashing)
public class ReconcileProcessFunction
        extends ProcessFunction<String, ReconcileMessage> {

    private transient ObjectMapper mapper;
    private transient CqlSession session;

    @Override
    public void open(Configuration parameters) {
        mapper = new ObjectMapper();

        session = CqlSession.builder()
                .addContactPoint(
                    new InetSocketAddress("cassandra-host", 9042))
                .withLocalDatacenter("dc1")
                .build();
    }

    @Override
    public void processElement(
            String value,
            Context ctx,
            Collector<ReconcileMessage> out) throws Exception {

        InputMessage input =
                mapper.readValue(value, InputMessage.class);

        // Query Cassandra
        String query =
                String.format(
                        "SELECT * FROM %s.%s WHERE pk = ?",
                        input.keyspace,
                        input.tableName);

        Row row =
                session.execute(
                        session.prepare(query)
                               .bind(input.pk))
                        .one();

        String cassandraRowJson =
                (row == null) ? "" : row.toString();

        // Hashes
        String kafkaHash = sha256(value);
        String cassandraHash = sha256(cassandraRowJson);

        ReconcileMessage output = new ReconcileMessage();
        output.pk = input.pk;
        output.hashKafkaMessage = kafkaHash;
        output.hashCassandraRow = cassandraHash;
        output.payload = input.payload;

        out.collect(output);
    }

    @Override
    public void close() {
        if (session != null) {
            session.close();
        }
    }

    private String sha256(String input) throws Exception {
        MessageDigest digest =
                MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(
                input.getBytes(StandardCharsets.UTF_8));

        return DatatypeConverter.printHexBinary(hash);
    }
}


## 8. 8. Important production notes ⚠️
This works, but for real systems you should:
Do NOT open Cassandra session per record
   We open once in open() (✔ correct)
Prepare statements once
   Cache PreparedStatement in open()
Async Cassandra calls
   Use executeAsync() + AsyncFunction for throughput
Backpressure
   Cassandra is now in your hot path
Primary key type
   Adjust binding if PK is composite

================================================
#### 9. Want the next level?
Convert this to Async I/O with Cassandra
Add exactly-once Kafka semantics
## Skip for now => Add schema-based row hashing (stable order)
Support dynamic PKs / composite keys   


### 2nd INtegration: ==============
## 1. Full pipeline summary
Kafka(input-topic, EOS)
   ↓
Async Cassandra Lookup
   ↓
Hashing & Reconciliation
   ↓
Kafka(reconcile, EOS)
## Sum Total:
V Cassandra is read-only → safe with EOS
✔ Async I/O prevents backpressure
✔ Composite PKs supported
✔ Stable, restart-safe output


## KAfka message input-kafka
{
  "keyspace": "ks1",
  "tableName": "users",
  "primaryKey": {
    "user_id": "123",
    "region": "us-east"
  },
  "payload": {
    "name": "alice",
    "age": 30
  }
}

## 2. Updated POJOs InputMessage
public class InputMessage {
    public String keyspace;
    public String tableName;
    public Map<String, Object> primaryKey;
    public JsonNode payload;
}
## Output :
public class ReconcileMessage {
    public Map<String, Object> primaryKey;
    public String hashKafkaMessage;
    public String hashCassandraRow;
    public JsonNode payload;
}

3. Async Cassandra lookup (core change)
public class CassandraAsyncLookup
        extends RichAsyncFunction<String, ReconcileMessage> {

    private transient ObjectMapper mapper;
    private transient CqlSession session;

    @Override
    public void open(Configuration parameters) {
        mapper = new ObjectMapper();

        session = CqlSession.builder()
                .addContactPoint(
                    new InetSocketAddress("cassandra-host", 9042))
                .withLocalDatacenter("dc1")
                .build();
    }

    @Override
    public void asyncInvoke(
            String value,
            ResultFuture<ReconcileMessage> resultFuture) {

        try {
            InputMessage input =
                    mapper.readValue(value, InputMessage.class);

            String whereClause = input.primaryKey.keySet()
                    .stream()
                    .map(k -> k + " = ?")
                    .collect(Collectors.joining(" AND "));

            String cql = String.format(
                    "SELECT * FROM %s.%s WHERE %s",
                    input.keyspace,
                    input.tableName,
                    whereClause);

            PreparedStatement ps = session.prepare(cql);

            BoundStatementBuilder builder = ps.boundStatementBuilder();
            input.primaryKey.values().forEach(builder::setObject);

            CompletionStage<AsyncResultSet> future =
                    session.executeAsync(builder.build());

            future.whenComplete((rs, err) -> {
                if (err != null) {
                    resultFuture.completeExceptionally(err);
                    return;
                }

                Row row = rs.one();
                String cassandraRow =
                        row == null ? "" : row.toString();

                ReconcileMessage out = new ReconcileMessage();
                out.primaryKey = input.primaryKey;
                out.payload = input.payload;
                out.hashKafkaMessage = sha256(value);
                out.hashCassandraRow = sha256(cassandraRow);

                resultFuture.complete(
                        Collections.singleton(out));
            });

        } catch (Exception e) {
            resultFuture.completeExceptionally(e);
        }
    }

    @Override
    public void close() {
        if (session != null) {
            session.close();
        }
    }

    private String sha256(String input) {
        try {
            MessageDigest digest =
                    MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(
                    input.getBytes(StandardCharsets.UTF_8));
            return DatatypeConverter.printHexBinary(hash);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}


## 4. Wiring Async I/O into the Flink job
DataStream<ReconcileMessage> reconciled =
    AsyncDataStream.unorderedWait(
        rawStream,
        new CassandraAsyncLookup(),
        5, TimeUnit.SECONDS,
        100  // max async requests in flight
    );

5. Exactly-once Kafka semantics (critical)
KafkaSource<String> source =
    KafkaSource.<String>builder()
        .setBootstrapServers("kafka:9092")
        .setTopics("input-topic")
        .setGroupId("reconcile-group")
        .setStartingOffsets(OffsetsInitializer.committedOffsets())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

6. Kafka Sink (Exactly-once)
KafkaSink<String> sink =
    KafkaSink.<String>builder()
        .setBootstrapServers("kafka:9092")
        .setRecordSerializer(
            KafkaRecordSerializationSchema.builder()
                .setTopic("reconcile")
                .setValueSerializationSchema(
                    new SimpleStringSchema())
                .build())
        .setDeliveryGuarantee(
            DeliveryGuarantee.EXACTLY_ONCE)
        .setTransactionalIdPrefix(
            "reconcile-tx")
        .build();

## ⚠️ Kafka requirements:
   transaction.state.log.replication.factor >= 3
   transaction.state.log.min.isr >= 2
   Kafka ≥ 0.11

##    6. Enable checkpoints (mandatory for EOS)
env.enableCheckpointing(30000); // 30s
env.getCheckpointConfig()
   .setCheckpointingMode(
       CheckpointingMode.EXACTLY_ONCE);

env.getCheckpointConfig()
   .setMinPauseBetweenCheckpoints(15000);

env.getCheckpointConfig()
   .setCheckpointTimeout(60000);


## ---------------------------------
8. Production hardening (recommended next)
   If this is going to prod, next steps should be:
   Cache PreparedStatement per (keyspace, table)
   Stable row hashing (column-order independent)
   Rate limiting Cassandra calls
   Timeout + retry strategy
   Metrics for Cassandra latency & failures
##   TODO:
   Add prepared statement cache
   ## Skip for now -> Add schema-aware row hashing
   Convert this to Flink SQL
   Add dead-letter topic

-------------------------
## PreparedStatement cache implementation 
>> Because: Preparing per record will destroy Cassandra
>> Cache by (keyspace, table, pkColumns)

public class PreparedStatementCache {

    private final CqlSession session;
    private final ConcurrentHashMap<String, PreparedStatement> cache =
            new ConcurrentHashMap<>();

    public PreparedStatementCache(CqlSession session) {
        this.session = session;
    }

    public PreparedStatement get(
            String keyspace,
            String table,
            List<String> pkColumns) {

        String cacheKey =
                keyspace + "." + table + ":" +
                String.join(",", pkColumns);

        return cache.computeIfAbsent(cacheKey, k -> {
            String whereClause = pkColumns.stream()
                    .map(c -> c + " = ?")
                    .collect(Collectors.joining(" AND "));

            String cql = String.format(
                    "SELECT * FROM %s.%s WHERE %s",
                    keyspace,
                    table,
                    whereClause);

            return session.prepare(cql);
        });
    }
}


## Updated Async Cassandra Function (cached)
public class CassandraAsyncLookup
        extends RichAsyncFunction<String, ReconcileMessage> {

    private transient ObjectMapper mapper;
    private transient CqlSession session;
    private transient PreparedStatementCache psCache;

    @Override
    public void open(Configuration parameters) {
        mapper = new ObjectMapper();
        session = CqlSession.builder()
                .addContactPoint(
                    new InetSocketAddress("cassandra-host", 9042))
                .withLocalDatacenter("dc1")
                .build();

        psCache = new PreparedStatementCache(session);
    }

    @Override
    public void asyncInvoke(
            String value,
            ResultFuture<ReconcileMessage> resultFuture) {

        try {
            InputMessage input =
                    mapper.readValue(value, InputMessage.class);

            List<String> pkColumns =
                    new ArrayList<>(input.primaryKey.keySet());

            PreparedStatement ps =
                    psCache.get(
                        input.keyspace,
                        input.tableName,
                        pkColumns);

            BoundStatementBuilder bs =
                    ps.boundStatementBuilder();

            for (String col : pkColumns) {
                bs.setObject(col, input.primaryKey.get(col));
            }

            session.executeAsync(bs.build())
                   .whenComplete((rs, err) -> {
                       if (err != null) {
                           resultFuture.completeExceptionally(err);
                           return;
                       }

                       Row row = rs.one();
                       String cassandraRow =
                               row == null ? "" : row.toString();

                       ReconcileMessage out = new ReconcileMessage();
                       out.primaryKey = input.primaryKey;
                       out.payload = input.payload;
                       out.hashKafkaMessage = sha256(value);
                       out.hashCassandraRow = sha256(cassandraRow);

                       resultFuture.complete(
                           Collections.singleton(out));
                   });

        } catch (Exception e) {
            resultFuture.completeExceptionally(e);
        }
    }
}


## ########################################################
Final Architecture (clean + safe)
Kafka(input-topic, EOS)
   ↓
Async Cassandra Lookup (cached, non-blocking)
   ↓
Reconcile Stream
   ├── Kafka(reconcile, exactly-once)
   └── Kafka(reconcile-dlq)


## 2. Flink SQL version (with Cassandra async lookup)
⚠️ Important truth:
Flink SQL does NOT have a native Cassandra lookup connector.
So the correct approach is:

# Step 1: Register Kafka source table
CREATE TABLE kafka_input (
  keyspace STRING,
  tableName STRING,
  primaryKey MAP<STRING, STRING>,
  payload STRING,
  raw STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'input-topic',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'reconcile-sql',
  'format' = 'json',
  'scan.startup.mode' = 'group-offsets'
);
# Step 2: Cassandra async lookup UDF
public class CassandraHashUdf
        extends AsyncScalarFunction {

    private transient CqlSession session;
    private transient PreparedStatementCache psCache;

    @Override
    public void open(FunctionContext context) {
        session = CqlSession.builder()
                .addContactPoint(
                    new InetSocketAddress("cassandra-host", 9042))
                .withLocalDatacenter("dc1")
                .build();

        psCache = new PreparedStatementCache(session);
    }

    public void eval(
            CompletableFuture<String> future,
            String keyspace,
            String table,
            Map<String, String> pk) {

        List<String> pkCols =
                new ArrayList<>(pk.keySet());

        PreparedStatement ps =
                psCache.get(keyspace, table, pkCols);

        BoundStatementBuilder bs =
                ps.boundStatementBuilder();

        pk.forEach(bs::setObject);

        session.executeAsync(bs.build())
               .whenComplete((rs, err) -> {
                   if (err != null) {
                       future.complete("");
                       return;
                   }
                   Row row = rs.one();
                   future.complete(
                       row == null ? "" : sha256(row.toString()));
               });
    }
}

## Register it:
tableEnv.createTemporarySystemFunction(
    "cassandra_hash",
    CassandraHashUdf.class);

# Step 3: Kafka sink table (exactly-once)
CREATE TABLE kafka_reconcile (
  primaryKey MAP<STRING, STRING>,
  hash_kafka STRING,
  hash_cassandra STRING,
  payload STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'reconcile',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json',
  'sink.delivery-guarantee' = 'exactly-once'
);

# Step 4: SQL pipeline
INSERT INTO kafka_reconcile
SELECT
  primaryKey,
  SHA256(raw) AS hash_kafka,
  cassandra_hash(keyspace, tableName, primaryKey) AS hash_cassandra,
  payload
FROM kafka_input;

# 3. Dead-Letter Topic (DLQ)
Strategy
   Never crash the job for bad data
   Route failures to a Kafka DLQ

## DLQ message format
{
  "error": "Cassandra timeout",
  "originalMessage": "...",
  "timestamp": 1700000000
}

# DLQ OutputTag
OutputTag<String> DLQ =
    new OutputTag<>("dlq", TypeInformation.of(String.class));

##  Modified Async Function with DLQ   
if (err != null) {
    String dlqMsg = mapper.writeValueAsString(
        Map.of(
            "error", err.getMessage(),
            "originalMessage", value,
            "timestamp", System.currentTimeMillis()
        )
    );
    ctx.output(DLQ, dlqMsg);
    resultFuture.complete(Collections.emptyList());
    return;
}

## DLQ Kafka Sink
KafkaSink<String> dlqSink =
    KafkaSink.<String>builder()
        .setBootstrapServers("kafka:9092")
        .setRecordSerializer(
            KafkaRecordSerializationSchema.builder()
                .setTopic("reconcile-dlq")
                .setValueSerializationSchema(
                    new SimpleStringSchema())
                .build())
        .build();

sideOutputStream.sinkTo(dlqSink);


#### ---------NEXT STEPS-----------
Stable schema-based hashing
Cassandra token-aware routing
Rate-limited async pool
Full Flink SQL only solution (no DataStream)
------------------------------------------

## ############################################################
## Full Flink SQL only solution (no DataStream) ##
---------------------------------------------------------------
“SQL-only” means:
   Pipeline logic = 100% SQL
   External I/O (Cassandra) = Async SQL UDF
   No DataStream operators in the job graph
-----
## Still need a small Java JAR for:
   Async Cassandra lookup UDF
   PreparedStatement cache
   Hashingcalling
   - But once registered, everything else is pure SQL.
-----
This is exactly how Flink SQL is intended to be used for non-native connectors.   
------------------------
## Final Architecture (SQL-only pipeline)
Kafka (input-topic)
   ↓
Flink SQL Table
   ↓
Async Cassandra Lookup UDFFlink
   ↓
Hashing (SQL + UDF)
   ↓
Kafyep will doka (reconcile, exactly-once)
   ↓
Kafka (reconcile-dlq)
