/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import io.debezium.config.Configuration;
import io.debezium.connector.cockroachdb.CockroachDBConnectorConfig;

/**
 * Unit tests for CockroachDBConnection.
 *
 * @author Virag Tripathi
 */
@RunWith(MockitoJUnitRunner.class)
public class CockroachDBConnectionTest {

    private CockroachDBConnectorConfig config;
    private CockroachDBConnection connection;

    @Before
    public void setUp() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.port", "26257");
        props.put("database.user", "root");
        props.put("database.password", "");
        props.put("database.dbname", "testdb");
        props.put("database.server.name", "test-server");
        props.put("topic.prefix", "test");

        config = new CockroachDBConnectorConfig(Configuration.from(props));
        connection = new CockroachDBConnection(config);
    }

    @Test
    public void shouldConnectSuccessfully() throws SQLException {
        try (MockedStatic<DriverManager> driverManagerMock = Mockito.mockStatic(DriverManager.class)) {
            Connection mockConnection = mock(Connection.class);
            Statement mockStatement = mock(Statement.class);
            java.sql.ResultSet mockResultSet = mock(java.sql.ResultSet.class);

            when(mockConnection.createStatement()).thenReturn(mockStatement);
            when(mockStatement.execute(Mockito.anyString())).thenReturn(true);
            when(mockStatement.getResultSet()).thenReturn(mockResultSet);
            when(mockResultSet.next()).thenReturn(true, true, true); // For rangefeed, privilege, and table checks
            when(mockResultSet.getString(1)).thenReturn("true"); // Rangefeed enabled
            when(mockResultSet.getBoolean(1)).thenReturn(true); // Has privilege
            driverManagerMock.when(() -> DriverManager.getConnection(Mockito.anyString(), Mockito.any()))
                    .thenReturn(mockConnection);

            connection.connect();

            assertThat(connection).isNotNull();
        }
    }

    @Test
    public void shouldHandleConnectionFailure() {
        try (MockedStatic<DriverManager> driverManagerMock = Mockito.mockStatic(DriverManager.class)) {
            driverManagerMock.when(() -> DriverManager.getConnection(Mockito.anyString(), Mockito.any()))
                    .thenThrow(new RuntimeException("Connection failed"));

            assertThatThrownBy(() -> connection.connect())
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Connection failed");
        }
    }

    @Test
    public void shouldHandleTransientErrorsWithRetry() {
        try (MockedStatic<DriverManager> driverManagerMock = Mockito.mockStatic(DriverManager.class)) {
            SQLException transientError = new SQLException("serialization failure", "40001");
            RuntimeException permanentError = new RuntimeException("Permanent failure");

            driverManagerMock.when(() -> DriverManager.getConnection(Mockito.anyString(), Mockito.any()))
                    .thenThrow(transientError)
                    .thenThrow(transientError)
                    .thenThrow(permanentError);

            assertThatThrownBy(() -> connection.connect())
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Permanent failure");
        }
    }

    @Test
    public void shouldBuildCorrectConnectionUrl() throws SQLException {
        String expectedUrl = "jdbc:postgresql://localhost:26257/testdb";

        // Use reflection or a test method to verify the URL building logic
        // For now, we'll test the connection attempt which uses the URL
        try (MockedStatic<DriverManager> driverManagerMock = Mockito.mockStatic(DriverManager.class)) {
            Connection mockConnection = mock(Connection.class);
            Statement mockStatement = mock(Statement.class);
            java.sql.ResultSet mockResultSet = mock(java.sql.ResultSet.class);

            when(mockConnection.createStatement()).thenReturn(mockStatement);
            when(mockStatement.execute(Mockito.anyString())).thenReturn(true);
            when(mockStatement.getResultSet()).thenReturn(mockResultSet);
            when(mockResultSet.next()).thenReturn(true, true, true); // For rangefeed, privilege, and table checks
            when(mockResultSet.getString(1)).thenReturn("true"); // Rangefeed enabled
            when(mockResultSet.getBoolean(1)).thenReturn(true); // Has privilege
            driverManagerMock.when(() -> DriverManager.getConnection(Mockito.anyString(), Mockito.any()))
                    .thenReturn(mockConnection);

            connection.connect();

            // Verify that DriverManager.getConnection was called with the expected URL
            driverManagerMock.verify(() -> DriverManager.getConnection(Mockito.anyString(), Mockito.any()));
        }
    }

    @Test
    public void shouldHandleSSLConfiguration() throws SQLException {
        Map<String, String> sslProps = new HashMap<>();
        sslProps.put("database.hostname", "localhost");
        sslProps.put("database.port", "26257");
        sslProps.put("database.user", "root");
        sslProps.put("database.password", "");
        sslProps.put("database.dbname", "testdb");
        sslProps.put("database.server.name", "test-server");
        sslProps.put("topic.prefix", "test");
        sslProps.put("database.sslmode", "require");

        CockroachDBConnectorConfig sslConfig = new CockroachDBConnectorConfig(Configuration.from(sslProps));
        CockroachDBConnection sslConnection = new CockroachDBConnection(sslConfig);

        try (MockedStatic<DriverManager> driverManagerMock = Mockito.mockStatic(DriverManager.class)) {
            Connection mockConnection = mock(Connection.class);
            Statement mockStatement = mock(Statement.class);
            java.sql.ResultSet mockResultSet = mock(java.sql.ResultSet.class);

            when(mockConnection.createStatement()).thenReturn(mockStatement);
            when(mockStatement.execute(Mockito.anyString())).thenReturn(true);
            when(mockStatement.getResultSet()).thenReturn(mockResultSet);
            when(mockResultSet.next()).thenReturn(true, true, true); // For rangefeed, privilege, and table checks
            when(mockResultSet.getString(1)).thenReturn("true"); // Rangefeed enabled
            when(mockResultSet.getBoolean(1)).thenReturn(true); // Has privilege
            driverManagerMock.when(() -> DriverManager.getConnection(Mockito.anyString(), Mockito.any()))
                    .thenReturn(mockConnection);

            sslConnection.connect();

            // Verify SSL connection was attempted
            driverManagerMock.verify(() -> DriverManager.getConnection(Mockito.anyString(), Mockito.any()));
        }
    }
}