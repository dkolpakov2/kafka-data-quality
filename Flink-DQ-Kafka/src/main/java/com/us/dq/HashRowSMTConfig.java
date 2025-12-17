package com.us.dq;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

public class HashRowSMTConfig extends AbstractConfig {

    public static final String HASH_FIELDS = "hash.fields";
    public static final String HASH_ALGORITHM = "hash.algorithm";
    public static final String OUTPUT_FIELD = "output.field";

    public HashRowSMTConfig(Map<String, ?> originals) {
        super(config(), originals);
    }

    public static ConfigDef config() {
        return new ConfigDef()
            .define(
                HASH_FIELDS,
                ConfigDef.Type.LIST,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH,
                "Fields used to compute the hash (comma-separated)"
            )
            .define(
                HASH_ALGORITHM,
                ConfigDef.Type.STRING,
                "SHA-256",
                ConfigDef.Importance.MEDIUM,
                "Hash algorithm: SHA-256, SHA-512, MD5"
            )
            .define(
                OUTPUT_FIELD,
                ConfigDef.Type.STRING,
                "hash_cloud",
                ConfigDef.Importance.HIGH,
                "Output field to store the computed hash"
            );
    }

    public List<String> hashFields() {
        return getList(HASH_FIELDS);
    }

    public String algorithm() {
        return getString(HASH_ALGORITHM);
    }

    public String outputField() {
        return getString(OUTPUT_FIELD);
    }
}

