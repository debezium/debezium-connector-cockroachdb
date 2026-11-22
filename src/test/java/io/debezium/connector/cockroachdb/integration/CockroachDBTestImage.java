/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb.integration;

import org.testcontainers.utility.DockerImageName;

/**
 * The CockroachDB image that the integration tests start.
 *
 * @author Virag Tripathi
 */
public final class CockroachDBTestImage {

    /**
     * The CockroachDB version of the tests. The build passes it as the {@code cockroachdb.version}
     * system property, which {@code pom.xml} defines. The default applies to a test that is started
     * without the build, for example from an IDE.
     */
    public static final String VERSION = System.getProperty("cockroachdb.version", "v25.4.21");

    public static final DockerImageName NAME = DockerImageName.parse("cockroachdb/cockroach:" + VERSION);

    private CockroachDBTestImage() {
    }
}
