/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/**
 * Basic tests for the CockroachDB connector.
 *
 * @author Virag Tripathi
 */
public class CockroachDBConnectorTest {

    private static final String HOSTNAME = RelationalDatabaseConnectorConfig.HOSTNAME.name();
    private static final String LOCAL_ADDRESS = "127.0.0.1";

    @Test
    public void shouldHaveValidConfiguration() {
        CockroachDBConnector connector = new CockroachDBConnector();
        ConfigDef configDef = connector.config();

        assertThat(configDef).isNotNull();
        assertThat(configDef.names()).contains("database.hostname");
        assertThat(configDef.names()).contains("database.port");
        assertThat(configDef.names()).contains("database.user");
        assertThat(configDef.names()).contains("database.password");
        assertThat(configDef.names()).contains("database.dbname");
    }

    @Test
    public void shouldCreateValidConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.port", "26257");
        props.put("database.user", "root");
        props.put("database.password", "");
        props.put("database.dbname", "defaultdb");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");

        Configuration config = Configuration.from(props);
        CockroachDBConnectorConfig connectorConfig = new CockroachDBConnectorConfig(config);

        assertThat(connectorConfig.getDatabaseName()).isEqualTo("defaultdb");
        assertThat(connectorConfig.getLogicalName()).isEqualTo("test");
    }

    @Test
    public void shouldHaveCorrectVersion() {
        CockroachDBConnector connector = new CockroachDBConnector();
        String version = connector.version();

        assertThat(version).isNotNull();
        assertThat(version).isNotEmpty();
    }

    @Test
    public void shouldHaveCorrectTaskClass() {
        CockroachDBConnector connector = new CockroachDBConnector();

        assertThat(connector.taskClass()).isEqualTo(CockroachDBConnectorTask.class);
    }

    @Test
    public void shouldGenerateTaskConfigs() {
        CockroachDBConnector connector = new CockroachDBConnector();

        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.server.name", "test-server");

        connector.start(props);

        var taskConfigs = connector.taskConfigs(1);
        assertThat(taskConfigs).hasSize(1);
        assertThat(taskConfigs.get(0)).containsKey("database.hostname");
        assertThat(taskConfigs.get(0).get("database.hostname")).isEqualTo("localhost");
    }

    @Test
    public void shouldReportAnInvalidValueWhenTheConfigurationIsValidated() throws Exception {
        final Map<String, String> props = propertiesOfADatabaseThatDoesNotListen();
        props.put(CockroachDBConnectorConfig.CHANGEFEED_SINK_TYPE.name(), "webhook");

        final Config result = new CockroachDBConnector().validate(props);

        assertThat(errorsOf(result, CockroachDBConnectorConfig.CHANGEFEED_SINK_TYPE.name()))
                .anyMatch(message -> message.contains("Must be 'kafka' or 'sinkless'"));
    }

    @Test
    public void shouldReportAMissingRequiredPropertyWhenTheConfigurationIsValidated() throws Exception {
        final Map<String, String> props = propertiesOfADatabaseThatDoesNotListen();
        props.remove(RelationalDatabaseConnectorConfig.DATABASE_NAME.name());

        final Config result = new CockroachDBConnector().validate(props);

        assertThat(errorsOf(result, RelationalDatabaseConnectorConfig.DATABASE_NAME.name())).isNotEmpty();
    }

    @Test
    public void shouldReportAClassThatCannotBeLoadedWhenTheConfigurationIsValidated() throws Exception {
        // Kafka Connect's check of the property types reports it. The field validators only look at the
        // form of a class name.
        final Map<String, String> props = propertiesOfADatabaseThatDoesNotListen();
        props.put(CommonConnectorConfig.TOPIC_NAMING_STRATEGY.name(), "io.debezium.connector.cockroachdb.NoSuchStrategy");

        final Config result = new CockroachDBConnector().validate(props);

        assertThat(errorsOf(result, CommonConnectorConfig.TOPIC_NAMING_STRATEGY.name()))
                .anyMatch(message -> message.contains("NoSuchStrategy"));
    }

    @Test
    public void shouldNotConnectWhenAPropertyIsInvalid() throws Exception {
        // Nothing listens on the port, so an attempt to connect would add an error to the host name.
        final Map<String, String> props = propertiesOfADatabaseThatDoesNotListen();
        props.put(CockroachDBConnectorConfig.CHANGEFEED_SINK_TYPE.name(), "webhook");

        final Config result = new CockroachDBConnector().validate(props);

        assertThat(errorsOf(result, HOSTNAME)).isEmpty();
    }

    @Test
    public void shouldReportAConnectionThatFailsWhenTheConfigurationIsValidated() throws Exception {
        final Config result = new CockroachDBConnector().validate(propertiesOfADatabaseThatDoesNotListen());

        assertThat(errorsOf(result, HOSTNAME)).anyMatch(message -> message.startsWith("Unable to connect: "));
        assertThat(result.configValues())
                .filteredOn(value -> !value.name().equals(HOSTNAME))
                .allSatisfy(value -> assertThat(value.errorMessages()).as(value.name()).isEmpty());
    }

    @Test
    @Timeout(8)
    public void shouldTryTheConnectionOnceWhenTheConfigurationIsValidated() throws Exception {
        // The task tries a refused connection again. A validation that did the same would answer after the
        // waits between the attempts, here 10, 20, 30 and 40 seconds, or give up without the reason.
        final Map<String, String> props = propertiesOfADatabaseThatDoesNotListen();
        props.put(CockroachDBConnectorConfig.CONNECTION_MAX_RETRIES.name(), "5");
        props.put(CockroachDBConnectorConfig.CONNECTION_RETRY_DELAY_MS.name(), "10000");

        final Config result = new CockroachDBConnector().validate(props);

        assertThat(errorsOf(result, HOSTNAME)).anyMatch(message -> message.startsWith("Unable to connect: "));
    }

    @Test
    @Timeout(30)
    public void shouldGiveUpWhenTheDatabaseDoesNotAnswerInTime() throws Exception {
        // A server that accepts the connection and never answers.
        try (ServerSocket silent = new ServerSocket(0, 1, InetAddress.getByName(LOCAL_ADDRESS))) {
            final Map<String, String> props = propertiesOfADatabaseAt(silent.getLocalPort());
            props.put(CommonConnectorConfig.CONNECTION_VALIDATION_TIMEOUT_MS.name(), "1000");

            final Config result = new CockroachDBConnector().validate(props);

            assertThat(errorsOf(result, HOSTNAME)).containsExactly("Connection validation timed out after 1000 ms");

            // The attempt to connect ends when the server goes away. The validation has returned its
            // result by then, and the result must stay as it is.
            silent.close();
            await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(5))
                    .until(() -> errorsOf(result, HOSTNAME).size() == 1);
        }
    }

    /** Properties of a database on a local port that has just been free, so a connection to it is refused. */
    @Test
    @Timeout(30)
    public void shouldNotKeepWaitingForADatabaseThatStopsAnswering() throws Exception {
        // A server that answers the request for TLS and then says nothing, like a node that accepts
        // connections while its cluster is not available. It keeps the connection open.
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getByName(LOCAL_ADDRESS))) {
            final Thread silentAfterTheFirstAnswer = new Thread(() -> {
                try (Socket client = server.accept()) {
                    client.getInputStream().readNBytes(8);
                    client.getOutputStream().write('N');
                    client.getOutputStream().flush();
                    while (client.getInputStream().read() >= 0) {
                        // reads what the client sends and never answers
                    }
                }
                catch (IOException e) {
                    // the test is over
                }
            }, "server-that-stops-answering");
            silentAfterTheFirstAnswer.setDaemon(true);
            silentAfterTheFirstAnswer.start();
            final Map<String, String> props = propertiesOfADatabaseAt(server.getLocalPort());
            props.put(CommonConnectorConfig.CONNECTION_VALIDATION_TIMEOUT_MS.name(), "1000");

            final Config result = new CockroachDBConnector().validate(props);

            assertThat(errorsOf(result, HOSTNAME)).containsExactly("Connection validation timed out after 1000 ms");
            // The thread that tried to connect must end on its own, also while the server keeps the
            // connection open. Otherwise every validation against such a server leaves a thread behind.
            await().atMost(Duration.ofSeconds(15)).until(() -> Thread.getAllStackTraces().keySet().stream()
                    .noneMatch(thread -> thread.getName().contains("connection-validation")));
            assertThat(silentAfterTheFirstAnswer.isAlive()).as("The server had no part in ending the attempt").isFalse();
        }
    }

    private static Map<String, String> propertiesOfADatabaseThatDoesNotListen() throws IOException {
        final int port;
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getByName(LOCAL_ADDRESS))) {
            port = socket.getLocalPort();
        }
        return propertiesOfADatabaseAt(port);
    }

    private static Map<String, String> propertiesOfADatabaseAt(int port) {
        final Map<String, String> props = new HashMap<>();
        props.put(HOSTNAME, LOCAL_ADDRESS);
        props.put(RelationalDatabaseConnectorConfig.PORT.name(), String.valueOf(port));
        props.put(RelationalDatabaseConnectorConfig.USER.name(), "root");
        props.put(RelationalDatabaseConnectorConfig.PASSWORD.name(), "");
        props.put(RelationalDatabaseConnectorConfig.DATABASE_NAME.name(), "defaultdb");
        props.put(CommonConnectorConfig.TOPIC_PREFIX.name(), "test");
        return props;
    }

    private static List<String> errorsOf(Config result, String property) {
        return result.configValues().stream()
                .filter(value -> value.name().equals(property))
                .map(ConfigValue::errorMessages)
                .findFirst()
                .orElseThrow(() -> new AssertionError("The validation result has no entry for " + property));
    }
}
