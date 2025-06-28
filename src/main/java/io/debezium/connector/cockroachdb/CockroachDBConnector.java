/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main connector class used to instantiate configuration and execution classes
 * Uses CockroachDB's native changefeed (enriched envelope) as the source of truth.
 * <p>
 * * @author Virag Tripathi
 */
public class CockroachDBConnector extends SourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBConnector.class);

    private Map<String, String> config;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = props;
        LOGGER.info("Starting CockroachDBConnector with config: {}", props);
    }

    @Override
    public Class<? extends SourceTask> taskClass() {
        return CockroachDBConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        LOGGER.info("Generating task configs for {} task(s)", maxTasks);
        return List.of(config); // Single task for now
    }

    @Override
    public void stop() {
        LOGGER.info("Stopping CockroachDBConnector.");
    }

    @Override
    public ConfigDef config() {
        return CockroachDBConnectorConfig.configDef();
    }
}
