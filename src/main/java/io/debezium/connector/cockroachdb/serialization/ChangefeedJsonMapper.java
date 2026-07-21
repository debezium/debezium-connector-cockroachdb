/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cockroachdb.serialization;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * Factory for the Jackson {@link ObjectMapper} used to parse changefeed JSON.
 *
 * <p>All changefeed payload parsing must go through this factory so numeric handling is
 * consistent across the connector. Changefeed JSON carries DECIMAL values as JSON numbers with
 * the full source precision; the default Jackson representation for them is a Java double,
 * which silently rounds anything above roughly 15 to 17 significant digits. Parsing them as
 * {@link java.math.BigDecimal} preserves the source digits exactly.</p>
 *
 * @author Virag Tripathi
 */
public final class ChangefeedJsonMapper {

    private ChangefeedJsonMapper() {
    }

    public static ObjectMapper create() {
        ObjectMapper mapper = new ObjectMapper()
                .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        // Keep the exact scale the changefeed emits (DECIMAL columns are padded to their
        // declared scale); the default node factory strips trailing zeros.
        mapper.setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
        return mapper;
    }
}
