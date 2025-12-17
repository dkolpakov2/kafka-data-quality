package com.us.dq;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public class HashRowSMT<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(HashRowSMT.class);

    private HashRowSMTConfig config;

    @Override
    public void configure(Map<String, ?> configs) {
        this.config = new HashRowSMTConfig(configs);
    }

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        }

        if (!(record.value() instanceof Map)) {
            log.warn("Record value is not a Map. Skipping hash.");
            return record;
        }

        Map<String, Object> value = new HashMap<>((Map<String, Object>) record.value());

        try {
            String canonical = buildCanonicalString(value);
            String hash = computeHash(canonical);

            value.put(config.outputField(), hash);

            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                value,
                record.timestamp()
            );
        } catch (Exception e) {
            log.error("Failed to compute hash", e);
            throw e; // Let Kafka Connect handle DLQ
        }
    }

    private String buildCanonicalString(Map<String, Object> value) {
        return config.hashFields().stream()
            .sorted()
            .map(f -> Objects.toString(value.get(f), ""))
            .collect(Collectors.joining("|"));
    }

    private String computeHash(String input) {
        switch (config.algorithm().toUpperCase()) {
            case "SHA-512":
                return DigestUtils.sha512Hex(input.getBytes(StandardCharsets.UTF_8));
            case "MD5":
                return DigestUtils.md5Hex(input.getBytes(StandardCharsets.UTF_8));
            case "SHA-256":
            default:
                return DigestUtils.sha256Hex(input.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public ConfigDef config() {
        return HashRowSMTConfig.config();
    }

    @Override
    public void close() {
        // nothing to close
    }
}
