/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.util.Strings;

/**
 * Configuration class for the CockroachDB connector.
 *
 * @author Virag Tripathi
 */
public class CockroachDBConnectorConfig extends RelationalDatabaseConnectorConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBConnectorConfig.class);

    protected static final String DATABASE_CONFIG_PREFIX = "database.";
    protected static final int DEFAULT_PORT = 26257;
    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = 1024;

    public static final Field PORT = RelationalDatabaseConnectorConfig.PORT.withDefault(DEFAULT_PORT);

    public static final Field SERVER_NAME = Field.create("database.server.name")
            .withDisplayName("Server name")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 0))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withDescription("Unique name that identifies the database server and all recorded offsets, and is used as a prefix for all Kafka topic names.");

    public static final Field ON_CONNECT_STATEMENTS = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.ON_CONNECT_STATEMENTS)
            .withDisplayName("Initial statements")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withDescription("A semicolon separated list of SQL statements to be executed when a JDBC connection to the database is established. "
                    + "Note that the connector may establish JDBC connections at its own discretion, so this should typically be used for configuration "
                    + "of session parameters only, but not for executing DML statements. Use doubled semicolon (';;') to use a semicolon as a character "
                    + "and not as a delimiter.");

    public static final Field SSL_MODE = Field.create(DATABASE_CONFIG_PREFIX + "sslmode")
            .withDisplayName("SSL mode")
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 0))
            .withEnum(SecureConnectionMode.class, SecureConnectionMode.PREFER)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("Whether to use an encrypted connection to Postgres. Options include: "
                    + "'disable' (the default) to use an unencrypted connection; "
                    + "'allow' to try and use an unencrypted connection first and, failing that, a secure (encrypted) connection; "
                    + "'prefer' (the default) to try and use a secure (encrypted) connection first and, failing that, an unencrypted connection; "
                    + "'require' to use a secure (encrypted) connection, and fail if one cannot be established; "
                    + "'verify-ca' like 'required' but additionally verify the server TLS certificate against the configured Certificate Authority "
                    + "(CA) certificates, or fail if no valid matching CA certificates are found; or "
                    + "'verify-full' like 'verify-ca' but additionally verify that the server certificate matches the host to which the connection is attempted.");

    public static final Field SSL_CLIENT_CERT = Field.create(DATABASE_CONFIG_PREFIX + "sslcert")
            .withDisplayName("SSL Client Certificate")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("File containing the SSL Certificate for the client. See the Postgres SSL docs for further information");

    public static final Field SSL_CLIENT_KEY = Field.create(DATABASE_CONFIG_PREFIX + "sslkey")
            .withDisplayName("SSL Client Key")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 4))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("File containing the SSL private key for the client. See the Postgres SSL docs for further information");

    public static final Field SSL_CLIENT_KEY_PASSWORD = Field.create(DATABASE_CONFIG_PREFIX + "sslpassword")
            .withDisplayName("SSL Client Key Password")
            .withType(Type.PASSWORD)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 2))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("Password to access the client private key from the file specified by 'database.sslkey'. See the Postgres SSL docs for further information");

    public static final Field SSL_ROOT_CERT = Field.create(DATABASE_CONFIG_PREFIX + "sslrootcert")
            .withDisplayName("SSL Root Certificate")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 3))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("File containing the root certificate(s) against which the server is validated. See the Postgres JDBC SSL docs for further information");

    public static final Field SSL_SOCKET_FACTORY = Field.create(DATABASE_CONFIG_PREFIX + "sslfactory")
            .withDisplayName("SSL Root Certificate")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 5))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription(
                    "A name of class to that creates SSL Sockets. Use org.postgresql.ssl.NonValidatingFactory to disable SSL validation in development environments");

    public static final Field TCP_KEEPALIVE = Field.create(DATABASE_CONFIG_PREFIX + "tcpKeepAlive")
            .withDisplayName("TCP keep-alive probe")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 0))
            .withDefault(true)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Enable or disable TCP keep-alive probe to avoid dropping TCP connection")
            .withValidation(Field::isBoolean);

    public static final Field STATUS_UPDATE_INTERVAL_MS = Field.create("status.update.interval.ms")
            .withDisplayName("Status update interval (ms)")
            .withType(ConfigDef.Type.INT)
            .withDefault(10_000)
            .withDescription("How often to send status updates to the server in milliseconds.");

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 0))
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Select one of the following snapshot options: "
                    + "'always': The connector runs a snapshot every time that it starts. After the snapshot completes, the connector begins to stream changes from the transaction log.; "
                    + "'initial' (default): If the connector does not detect any offsets for the logical server name, it runs a snapshot that captures the current full state of the configured tables. After the snapshot completes, the connector begins to stream changes from the transaction log. "
                    + "'initial_only': The connector performs a snapshot as it does for the 'initial' option, but after the connector completes the snapshot, it stops, and does not stream changes from the transaction log.; "
                    + "'never': The connector does not run a snapshot. Upon first startup, the connector immediately begins reading from the beginning of the transaction log. "
                    + "'exported': This option is deprecated; use 'initial' instead.; "
                    + "'custom': The connector loads a custom class  to specify how the connector performs snapshots. For more information, see Custom snapshotter SPI in the PostgreSQL connector documentation.");

    public static final Field SNAPSHOT_ISOLATION_MODE = Field.create("snapshot.isolation.mode")
            .withDisplayName("Snapshot isolation mode")
            .withEnum(SnapshotIsolationMode.class, SnapshotIsolationMode.SERIALIZABLE)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 1))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Controls which transaction isolation level is used. "
                    + "The default is '" + SnapshotIsolationMode.SERIALIZABLE.getValue()
                    + "', which means that serializable isolation level is used. "
                    + "When '" + SnapshotIsolationMode.READ_COMMITTED.getValue()
                    + "' is specified, connector runs the initial snapshot in READ COMMITTED isolation level. ");

    public static final Field SNAPSHOT_LOCKING_MODE = Field.create("snapshot.locking.mode")
            .withDisplayName("Snapshot locking mode")
            .withEnum(SnapshotLockingMode.class, SnapshotLockingMode.NONE)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 13))
            .withDescription("Controls how the connector holds locks on tables while performing the schema snapshot. The 'shared' "
                    + "which means the connector will hold a table lock that prevents exclusive table access for just the initial portion of the snapshot "
                    + "while the database schemas and other metadata are being read. The remaining work in a snapshot involves selecting all rows from "
                    + "each table, and this is done using a flashback query that requires no locks. However, in some cases it may be desirable to avoid "
                    + "locks entirely which can be done by specifying 'none'. This mode is only safe to use if no schema changes are happening while the "
                    + "snapshot is taken.");

    public static final Field SOURCE_INFO_STRUCT_MAKER = CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER
            .withDefault(CockroachDBSourceInfoStructMaker.class.getName());

    public static final Field UNAVAILABLE_VALUE_PLACEHOLDER = RelationalDatabaseConnectorConfig.UNAVAILABLE_VALUE_PLACEHOLDER
            .withDescription("Placeholder for unavailable values (e.g. for toasted columns).");

    public static final Field READ_ONLY_CONNECTION = Field.create("read.only")
            .withDisplayName("Read only connection")
            .withType(ConfigDef.Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 100))
            .withDefault(false)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Switched connector to use alternative methods to deliver signals to Debezium instead "
                    + "of writing to signaling table");

    private static final ConfigDefinition CONFIG_DEFINITION = RelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("CockroachDB")
            .type(
                    HOSTNAME,
                    PORT,
                    USER,
                    PASSWORD,
                    DATABASE_NAME,
                    SERVER_NAME,
                    ON_CONNECT_STATEMENTS,
                    SSL_MODE,
                    SSL_ROOT_CERT,
                    SSL_CLIENT_CERT,
                    SSL_CLIENT_KEY,
                    SSL_CLIENT_KEY_PASSWORD,
                    TCP_KEEPALIVE,
                    STATUS_UPDATE_INTERVAL_MS)
            .events(
                    SOURCE_INFO_STRUCT_MAKER)
            .connector(
                    SNAPSHOT_MODE,
                    SNAPSHOT_ISOLATION_MODE,
                    SNAPSHOT_LOCKING_MODE,
                    UNAVAILABLE_VALUE_PLACEHOLDER)
            .create();

    public static final Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    /**
     * The set of predefined Snapshotter options or aliases.
     */
    public enum SnapshotMode implements EnumeratedValue {

        /**
         * Always perform a snapshot when starting.
         */
        ALWAYS("always"),

        /**
         * Perform a snapshot only upon initial startup of a connector.
         */
        INITIAL("initial"),

        /**
         * Never perform a snapshot and only receive logical changes.
         * @deprecated to be removed in Debezium 3.0, replaced by {{@link #NO_DATA}}
         */
        @Deprecated
        NEVER("never"),

        /**
         * Never perform a snapshot and only receive logical changes.
         */
        NO_DATA("no_data"),

        /**
         * Perform a snapshot and then stop before attempting to receive any logical changes.
         */
        INITIAL_ONLY("initial_only"),

        /**
         * Perform a snapshot when it is needed.
         */
        WHEN_NEEDED("when_needed"),

        /**
         * Allows control over snapshots by setting connectors properties prefixed with 'snapshot.mode.configuration.based'.
         */
        CONFIGURATION_BASED("configuration_based"),

        /**
         * Inject a custom snapshotter, which allows for more control over snapshots.
         */
        CUSTOM("custom");

        private final String value;

        SnapshotMode(String value) {
            this.value = value;

        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SnapshotMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();

            // Handle deprecated "never" value by mapping to NO_DATA
            if ("never".equalsIgnoreCase(value)) {
                return NO_DATA;
            }

            for (SnapshotMode option : SnapshotMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SnapshotMode parse(String value, String defaultValue) {
            SnapshotMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    /**
     * The set of predefined snapshot isolation mode options.
     */
    public enum SnapshotIsolationMode implements EnumeratedValue {

        /**
         * This mode uses SERIALIZABLE isolation level.
         */
        SERIALIZABLE("serializable"),

        /**
         * This mode uses REPEATABLE READ isolation level.
         */
        REPEATABLE_READ("repeatable_read"),

        /**
         * This mode uses READ COMMITTED isolation level.
         */
        READ_COMMITTED("read_committed"),

        /**
         * This mode uses READ UNCOMMITTED isolation level.
         */
        READ_UNCOMMITTED("read_uncommitted");

        private final String value;

        SnapshotIsolationMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SnapshotIsolationMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotIsolationMode option : SnapshotIsolationMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value        the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SnapshotIsolationMode parse(String value, String defaultValue) {
            SnapshotIsolationMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    /**
     * The set of predefined SecureConnectionMode options or aliases.
     */
    public enum SecureConnectionMode implements EnumeratedValue {

        /**
         * Establish an unencrypted connection
         *
         * see the {@code sslmode} Postgres JDBC driver option
         */
        DISABLED("disable"),

        /**
         * Establish an unencrypted connection first.
         * Establish a secure connection next if an unencrypted connection cannot be established
         *
         * see the {@code sslmode} Postgres JDBC driver option
         */
        ALLOW("allow"),

        /**
         * Establish a secure connection first.
         * Establish an unencrypted connection next if a secure connection cannot be established
         *
         * see the {@code sslmode} Postgres JDBC driver option
         */
        PREFER("prefer"),

        /**
         * Establish a secure connection if the server supports secure connections.
         * The connection attempt fails if a secure connection cannot be established
         *
         * see the {@code sslmode} Postgres JDBC driver option
         */
        REQUIRED("require"),

        /**
         * Like REQUIRED, but additionally verify the server TLS certificate against the configured Certificate Authority
         * (CA) certificates. The connection attempt fails if no valid matching CA certificates are found.
         *
         * see the {@code sslmode} Postgres JDBC driver option
         */
        VERIFY_CA("verify-ca"),

        /**
         * Like VERIFY_CA, but additionally verify that the server certificate matches the host to which the connection is
         * attempted.
         *
         * see the {@code sslmode} Postgres JDBC driver option
         */
        VERIFY_FULL("verify-full");

        private final String value;

        SecureConnectionMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SecureConnectionMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SecureConnectionMode option : SecureConnectionMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SecureConnectionMode parse(String value, String defaultValue) {
            SecureConnectionMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    public enum SnapshotLockingMode implements EnumeratedValue {
        /**
         * This mode will lock in ACCESS SHARE MODE to avoid concurrent schema changes during the snapshot, and
         * this does not prevent writes to the table, but prevents changes to the table's schema.
         */
        SHARED("shared"),

        /**
         * This mode will avoid using ANY table locks during the snapshot process.
         * This mode should be used carefully only when no schema changes are to occur.
         */
        NONE("none"),

        CUSTOM("custom");

        private final String value;

        SnapshotLockingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be {@code null}
         * @return the matching option, or null if no match is found
         */
        public static SnapshotLockingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotLockingMode option : SnapshotLockingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be {@code null}
         * @param defaultValue the default value; may be {@code null}
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SnapshotLockingMode parse(String value, String defaultValue) {
            SnapshotLockingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    private final String databaseName;
    private final SnapshotMode snapshotMode;
    private final SnapshotIsolationMode snapshotIsolationMode;
    private final SnapshotLockingMode snapshotLockingMode;
    private final boolean readOnlyConnection;
    protected final Configuration config;

    public CockroachDBConnectorConfig(Configuration config) {
        super(
                config,
                new SystemTablesPredicate(),
                x -> x.schema() + "." + x.table(),
                DEFAULT_SNAPSHOT_FETCH_SIZE,
                ColumnFilterMode.SCHEMA,
                false);
        this.config = config;
        this.databaseName = config.getString(DATABASE_NAME);
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE));
        this.snapshotIsolationMode = SnapshotIsolationMode.parse(config.getString(SNAPSHOT_ISOLATION_MODE));
        this.snapshotLockingMode = SnapshotLockingMode.parse(config.getString(SNAPSHOT_LOCKING_MODE));
        this.readOnlyConnection = config.getBoolean(READ_ONLY_CONNECTION);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    protected int port() {
        return config.getInteger(PORT);
    }

    public String databaseName() {
        return config.getString(DATABASE_NAME);
    }

    @Override
    public SnapshotMode getSnapshotMode() {
        return this.snapshotMode;
    }

    public SnapshotIsolationMode getSnapshotIsolationMode() {
        return this.snapshotIsolationMode;
    }

    @Override
    public Optional<SnapshotLockingMode> getSnapshotLockingMode() {
        return Optional.ofNullable(snapshotLockingMode);
    }

    public Duration statusUpdateInterval() {
        return Duration.ofMillis(config.getLong(STATUS_UPDATE_INTERVAL_MS));
    }

    @Override
    public byte[] getUnavailableValuePlaceholder() {
        String placeholder = config.getString(UNAVAILABLE_VALUE_PLACEHOLDER);
        if (placeholder.startsWith("hex:")) {
            return Strings.hexStringToByteArray(placeholder.substring(4));
        }
        return placeholder.getBytes();
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        return getSourceInfoStructMaker(SOURCE_INFO_STRUCT_MAKER, Module.name(), Module.version(), this);
    }

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    private static class SystemTablesPredicate implements TableFilter {
        protected static final List<String> SYSTEM_SCHEMAS = Arrays.asList("pg_catalog", "information_schema");
        // these are tables that may be placed in the user's schema but are system tables. This typically includes modules
        // that install system tables such as the GEO module
        protected static final List<String> SYSTEM_TABLES = List.of("spatial_ref_sys");
        protected static final String TEMP_TABLE_SCHEMA_PREFIX = "pg_temp";

        @Override
        public boolean isIncluded(TableId t) {
            return t.schema() != null && !SYSTEM_SCHEMAS.contains(t.schema().toLowerCase()) &&
                    t.table() != null && !SYSTEM_TABLES.contains(t.table().toLowerCase()) &&
                    !t.schema().startsWith(TEMP_TABLE_SCHEMA_PREFIX);
        }
    }

    public String getSslMode() {
        return config.getString(SSL_MODE);
    }

    public String getSslRootCert() {
        return config.getString(SSL_ROOT_CERT);
    }

    public String getSslClientCert() {
        return config.getString(SSL_CLIENT_CERT);
    }

    public String getSslClientKey() {
        return config.getString(SSL_CLIENT_KEY);
    }

    public String getSslClientKeyPassword() {
        return config.getString(SSL_CLIENT_KEY_PASSWORD);
    }

    public boolean isTcpKeepAlive() {
        return config.getBoolean(TCP_KEEPALIVE);
    }

    public String getOnConnectStatements() {
        return config.getString(ON_CONNECT_STATEMENTS);
    }

    public boolean isReadOnlyConnection() {
        return config.getBoolean(READ_ONLY_CONNECTION);
    }

    public String getHostname() {
        return config.getString(HOSTNAME);
    }

    public int getPort() {
        return config.getInteger(PORT);
    }

    public String getUser() {
        return config.getString(USER);
    }

    public String getPassword() {
        return config.getString(PASSWORD);
    }
}
