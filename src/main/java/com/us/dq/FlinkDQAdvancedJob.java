package com.example.dq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


// Fetches JSON schema from Schema Registry REST endpoint (configurable via SCHEMA_REGISTRY_URL and SCHEMA_SUBJECT) and compiles a JSON Schema validator
// Loads rules.yaml (from classpath or mounted file)
// For incoming record:
//  Validate JSON against schema
//  Apply rules from YAML (required/regex/range)
//  Maintain per-key EWMA/EWVAR for anomaly detection (for numeric value)
//  Route records:
//      valid & not anomaly → Cassandra sink
//      invalid (schema or rule) → DLQ Kafka topic dq_failures
//      anomalies → anomalies Kafka topic (and optionally metric)

public class FlinkDQAdvancedJob {

    static class Rule {
        public String id;
        public String field;
        public String type; // required, regex, range
        public String pattern;
        public Double min;
        public Double max;
        public String message;
    }

    static class RulesConfig {
        public List<Rule> rules;
        public Map<String, Object> anomaly;
    }

    static class DQResult {
        public JsonNode node;
        public boolean valid;
        public List<String> errors;
        public boolean anomaly;
        public String key;

        public DQResult(JsonNode node, boolean valid, List<String> errors, boolean anomaly, String key){
            this.node = node; this.valid = valid; this.errors = errors; this.anomaly = anomaly; this.key = key;
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ObjectMapper mapper = new ObjectMapper();

        // config from env or defaults
        final String kafkaBootstrap = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "kafka:9092");
        final String schemaRegistryUrl = System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", "http://schemaregistry:8081");
        final String schemaSubject = System.getenv().getOrDefault("SCHEMA_SUBJECT", "customer-value");
        final String rulesPath = System.getenv().getOrDefault("RULES_PATH", "/rules.yaml");

        // 1) fetch JSON schema from Schema Registry (subject -> latest)
        String schemaJson = fetchLatestSchemaFromRegistry(schemaRegistryUrl, schemaSubject);
        JsonSchemaFactory factory = JsonSchemaFactory.builder(JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7)).objectMapper(mapper).build();
        JsonSchema jsonSchema = factory.getSchema(mapper.readTree(schemaJson));

        // 2) load rules.yaml
        RulesConfig rulesConfig = loadRules(rulesPath, mapper);

        // set up Kafka consumer
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaBootstrap);
        props.setProperty("group.id", "flink-dq-advanced");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "validated_input", new org.apache.flink.api.common.serialization.SimpleStringSchema(), props);
        consumer.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(30)));

        DataStream<String> raw = env.addSource(consumer);

        // map => parse JSON, validate schema, apply rules, anomaly detection
        DataStream<DQResult> dqStream = raw
            .map(new RichMapFunction<String, DQResult>() {
                private transient JsonSchema schema;
                private transient RulesConfig rc;
                private transient ObjectMapper om;

                // EWMA state descriptors
                private transient ValueStateDescriptor<Double> emaDesc;
                private transient ValueStateDescriptor<Double> varDesc;
                private transient ValueStateDescriptor<Long> countDesc;

                private double alpha;
                private double zThreshold;
                private String anomalyField;

                @Override
                public void open(Configuration parameters) throws Exception {
                    this.schema = jsonSchema; // from outer scope
                    this.rc = rulesConfig;
                    this.om = mapper;

                    // anomaly config
                    Map<String,Object> an = rc.anomaly;
                    this.anomalyField = (String) an.getOrDefault("field", "value");
                    this.alpha = ((Number) an.getOrDefault("alpha", 0.2)).doubleValue();
                    this.zThreshold = ((Number) an.getOrDefault("z_threshold", 4.0)).doubleValue();

                    // state descriptors (keyed state will be available if we keyBy before map)
                    emaDesc = new ValueStateDescriptor<>("ema", Double.class);
                    varDesc = new ValueStateDescriptor<>("ewvar", Double.class);
                    countDesc = new ValueStateDescriptor<>("cnt", Long.class);
                }

                @Override
                public DQResult map(String value) throws Exception {
                    JsonNode node;
                    List<String> errors = new ArrayList<>();
                    boolean valid = true;
                    boolean isAnomaly = false;
                    String key = null;
                    try {
                        node = om.readTree(value);
                    } catch (Exception e) {
                        errors.add("malformed-json");
                        return new DQResult(null, false, errors, false, null);
                    }

                    // optional key extraction (for per-key state)
                    if (node.has("id")) key = node.get("id").asText();

                    // 1) schema validation
                    Set<ValidationMessage> vMsgs = schema.validate(node);
                    if (!vMsgs.isEmpty()) {
                        valid = false;
                        errors.addAll(vMsgs.stream().map(ValidationMessage::getMessage).collect(Collectors.toList()));
                    }

                    // 2) rule engine (YAML)
                    for (Rule r : rc.rules) {
                        if ("required".equalsIgnoreCase(r.type)) {
                            if (!node.has(r.field) || node.get(r.field).isNull()) {
                                valid = false;
                                errors.add(r.message != null ? r.message : r.field + " required");
                            }
                        } else if ("regex".equalsIgnoreCase(r.type) && node.has(r.field) && !node.get(r.field).isNull()) {
                            String text = node.get(r.field).asText("");
                            if (!Pattern.compile(r.pattern).matcher(text).matches()) {
                                valid = false;
                                errors.add(r.message != null ? r.message : r.field + " regex fail");
                            }
                        } else if ("range".equalsIgnoreCase(r.type) && node.has(r.field) && node.get(r.field).isNumber()) {
                            double v = node.get(r.field).asDouble();
                            if ((r.min != null && v < r.min) || (r.max != null && v > r.max)) {
                                valid = false;
                                errors.add(r.message != null ? r.message : r.field + " out of range");
                            }
                        }
                        // custom rules can be added here...
                    }

                    // 3) anomaly detection (EWMA online)
                    // NOTE: keyed state only works when we run .keyBy(id) before this map.
                    // To keep this map stateless if not keyed, we skip anomaly if key == null
                    if (key != null && node.has(anomalyField) && node.get(anomalyField).isNumber()) {
                        double v = node.get(anomalyField).asDouble();

                        // Access keyed state
                        ValueState<Double> emaState = getRuntimeContext().getState(emaDesc);
                        ValueState<Double> varState = getRuntimeContext().getState(varDesc);
                        ValueState<Long> cntState = getRuntimeContext().getState(countDesc);

                        Double emaPrev = emaState.value();
                        Double varPrev = varState.value();
                        Long cntPrev = cntState.value();

                        if (emaPrev == null) {
                            // initialize
                            emaPrev = v;
                            varPrev = 0.0;
                            cntPrev = 1L;
                            emaState.update(emaPrev);
                            varState.update(varPrev);
                            cntState.update(cntPrev);
                            isAnomaly = false; // first observation not anomaly
                        } else {
                            // ewma update
                            double emaNew = alpha * v + (1 - alpha) * emaPrev;

                            // ewma of variance (approx): update using squared dev using previous ema
                            double dev = v - emaPrev;
                            double varNew = (1 - alpha) * varPrev + alpha * dev * dev;

                            // approximate standard deviation
                            double std = Math.sqrt(varNew + 1e-9);

                            double z = Math.abs((v - emaNew) / (std + 1e-9));

                            // update states
                            emaState.update(emaNew);
                            varState.update(varNew);
                            cntState.update(cntPrev + 1);

                            if (z > zThreshold) {
                                isAnomaly = true;
                            }
                        }
                    }

                    return new DQResult(node, valid, errors, isAnomaly, key);
                }
            })
            // key by id for correct per-key state for anomaly detection
            .keyBy(r -> r.key == null ? "NO_KEY" : r.key)
            ;

        // split streams: valid & not anomaly => cassandra, invalid => dlq kafka, anomaly => kafka anomalies topic
        // We'll map to string for kafka sink
        FlinkKafkaProducer<String> dlqProducer = new FlinkKafkaProducer<>(
                kafkaBootstrap, "dq_failures", new org.apache.flink.api.common.serialization.SimpleStringSchema());

        FlinkKafkaProducer<String> anomalyProducer = new FlinkKafkaProducer<>(
                kafkaBootstrap, "anomalies", new org.apache.flink.api.common.serialization.SimpleStringSchema());

        // write to DLQ
        dqStream
            .filter(r -> !r.valid)
            .map(r -> {
                Map<String,Object> out = new HashMap<>();
                out.put("errors", r.errors);
                out.put("payload", r.node == null ? null : r.node.toString());
                out.put("key", r.key);
                return mapper.writeValueAsString(out);
            })
            .addSink(dlqProducer);

        // write anomalies
        dqStream
            .filter(r -> r.valid && r.anomaly)
            .map(r -> {
                Map<String,Object> out = new HashMap<>();
                out.put("anomaly", true);
                out.put("payload", r.node.toString());
                out.put("key", r.key);
                return mapper.writeValueAsString(out);
            })
            .addSink(anomalyProducer);

        // write valid & not anomaly to Cassandra sink (map to Tuple3<id,ts,value>)
        DataStream<Tuple3<String, String, Double>> cassandraTuples = dqStream
            .filter(r -> r.valid && !r.anomaly)
            .map(r -> Tuple3.of(r.node.get("id").asText(), r.node.get("timestamp").asText(), r.node.get("value").asDouble()));

        CassandraSink.addSink(cassandraTuples)
                .setQuery("INSERT INTO dq.data (id, ts, value) VALUES (?, ?, ?);")
                .setHost("cassandra")
                .build();

        env.execute("Flink Advanced DQ (schema + rules + anomaly)");
    }

    private static RulesConfig loadRules(String rulesPath, ObjectMapper mapper) throws Exception {
        // load yaml from classpath or filesystem
        InputStream in;
        if (rulesPath.startsWith("/")) {
            in = new java.io.FileInputStream(rulesPath);
        } else {
            in = FlinkDQAdvancedJob.class.getResourceAsStream(rulesPath);
            if (in == null) {
                in = new java.io.FileInputStream(rulesPath);
            }
        }
        Yaml yaml = new Yaml();
        Map<String,Object> data = yaml.load(in);
        RulesConfig rc = new RulesConfig();
        // parse rules
        List<Rule> rlist = new ArrayList<>();
        List<Map<String,Object>> rawRules = (List<Map<String,Object>>) data.get("rules");
        if (rawRules != null) {
            for (Map<String,Object> m : rawRules) {
                Rule r = new Rule();
                r.id = (String) m.get("id");
                r.field = (String) m.get("field");
                r.type = (String) m.get("type");
                r.pattern = (String) m.get("pattern");
                r.message = (String) m.get("message");
                if (m.get("min") != null) r.min = ((Number)m.get("min")).doubleValue();
                if (m.get("max") != null) r.max = ((Number)m.get("max")).doubleValue();
                rlist.add(r);
            }
        }
        rc.rules = rlist;
        rc.anomaly = (Map<String,Object>) data.getOrDefault("anomaly", Map.of());
        return rc;
    }

    private static String fetchLatestSchemaFromRegistry(String registryUrl, String subject) throws Exception {
        // Confluent Schema Registry REST API: /subjects/{subject}/versions/latest
        String url = registryUrl;
        if (url.endsWith("/")) url = url.substring(0, url.length()-1);
        String target = url + "/subjects/" + subject + "/versions/latest";
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(target))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new RuntimeException("Failed to fetch schema: " + resp.statusCode() + " " + resp.body());
        }
        // response contains {"subject":"...","version":x,"id":y,"schema":"{\"type\":\"object\"...\"}"}
        ObjectMapper om = new ObjectMapper();
        JsonNode root = om.readTree(resp.body());
        String schemaStr = root.get("schema").asText();
        return schemaStr;
    }
}