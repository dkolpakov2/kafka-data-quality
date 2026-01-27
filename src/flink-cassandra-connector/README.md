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