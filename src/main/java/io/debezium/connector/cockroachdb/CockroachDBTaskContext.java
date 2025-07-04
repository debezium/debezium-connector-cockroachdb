/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.relational.TableId;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Context for CockroachDB connector task execution.
 * Manages configuration and schema access.
 *
 * @author Virag Tripathi
 */
public class CockroachDBTaskContext extends CdcSourceTaskContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBTaskContext.class);

    private final CockroachDBConnectorConfig config;
    private final CockroachDBSchema schema;
    private final TopicNamingStrategy<TableId> topicNamingStrategy;

    public CockroachDBTaskContext(
                                  CockroachDBConnectorConfig config,
                                  CockroachDBSchema schema,
                                  TopicNamingStrategy<TableId> topicNamingStrategy) {
        super(config, Collections.emptyMap(), () -> Collections.emptyList());
        this.config = config;
        this.schema = schema;
        this.topicNamingStrategy = topicNamingStrategy;
    }

    public CockroachDBConnectorConfig getConfig() {
        return config;
    }

    public CockroachDBSchema getSchema() {
        return schema;
    }

    public TopicNamingStrategy<TableId> getTopicNamingStrategy() {
        return topicNamingStrategy;
    }
}
