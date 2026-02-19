/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;

/**
 * Context for CockroachDB connector task execution.
 * Manages configuration and schema access.
 *
 * @author Virag Tripathi
 */
public class CockroachDBTaskContext extends CdcSourceTaskContext<CockroachDBConnectorConfig> {

    private final CockroachDBConnectorConfig config;

    public CockroachDBTaskContext(Configuration rawConfig, CockroachDBConnectorConfig config) {
        super(rawConfig, config, config.getCustomMetricTags());
        this.config = config;
    }

    public CockroachDBConnectorConfig getConfig() {
        return config;
    }
}
