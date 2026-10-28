package io.kestra.plugin.mongodb;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;

import io.kestra.core.serializers.JacksonMapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

/**
 * Pins the properties a flow can set on a connection.
 */
class MongoDbConnectionTest {
    @Test
    void uriIsTheOnlyProperty() {
        var mapper = JacksonMapper.ofJson();
        var description = mapper.getDeserializationConfig().introspect(mapper.constructType(MongoDbConnection.class));

        // Jackson reads any one-argument method whose name starts with "set" as a property setter
        assertThat(description.findProperties().stream().map(BeanPropertyDefinition::getName).toList(), contains("uri"));
    }
}
