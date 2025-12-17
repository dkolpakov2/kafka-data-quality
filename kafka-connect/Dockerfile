FROM confluentinc/cp-kafka-connect:7.5.0

USER root

# Create plugin directories
RUN mkdir -p /usr/share/java/plugins/cassandra-source \
    && mkdir -p /usr/share/java/plugins/hash-smt

# Copy Cassandra Source Connector
COPY plugins/cassandra-source/*.jar /usr/share/java/plugins/cassandra-source/

# Copy Custom Hash SMT
COPY plugins/hash-smt/*.jar /usr/share/java/plugins/hash-smt/

# Set plugin path
ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/java/plugins"

# Ensure permissions
RUN chown -R appuser:appuser /usr/share/java/plugins

USER appuser
