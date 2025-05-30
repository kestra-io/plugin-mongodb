package io.kestra.plugin.mongodb;

import com.google.common.collect.ImmutableMap;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class FindTest {
    @Inject
    private RunContextFactory runContextFactory;

    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Find find = Find.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.ofValue("mongodb://root:example@localhost:27017/?authSource=admin"))
                .build())
            .database(Property.ofValue("samples"))
            .collection(Property.ofValue("books"))
            .filter(ImmutableMap.of(
                "pageCount", ImmutableMap.of("$gt", 600)
            ))
            .sort(ImmutableMap.of(
                "pageCount", -1
            ))
            .projection(ImmutableMap.of(
                "title", 1,
                "publishedDate", 1,
                "pageCount", 1
            ))
            .build();

        Find.Output findOutput = find.run(runContext);

        assertThat(findOutput.getSize(), is(36L));
        assertThat(((Map<String, Object>) findOutput.getRows().get(0)).get("pageCount"), is(1101));
        assertThat(((Map<String, Object>) findOutput.getRows().get(0)).get("_id"), is(70));
        assertThat(((Map<String, Object>) findOutput.getRows().get(0)).size(), is(4));
        assertThat(((Map<String, Object>) findOutput.getRows().get(0)).get("publishedDate"), is(Instant.parse("2000-08-01T07:00:00Z")));

        assertThat(((Map<String, Object>) findOutput.getRows().get(1)).get("_id"), is(315));
    }

    @Test
    void shouldPreserveFieldOrder() throws Exception {
        RunContext runContext = runContextFactory.of();

        var insertOneOutput = InsertOne.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.ofValue("mongodb://root:example@localhost:27017/?authSource=admin"))
                .build())
            .database(Property.ofValue("samples"))
            .collection(Property.ofValue("books"))
            .document("{\"a\": 1, \"z\": 2, \"m\": 3}")
            .build()
            .run(runContext);

        assertThat(insertOneOutput.getInsertedId(), is(notNullValue()));

        Find find = Find.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.ofValue("mongodb://root:example@localhost:27017/?authSource=admin"))
                .build())
            .database(Property.ofValue("samples"))
            .collection(Property.ofValue("books"))
            .filter(Map.of(
                "_id", Map.of("$oid", insertOneOutput.getInsertedId())
            ))
            .store(Property.ofValue(false))
            .build();

        Find.Output output = find.run(runContext);

        System.out.println(output.getRows() + " " + output.getSize());
        assertThat(output.getSize(), is(1L));

        Map<String, Object> doc = (Map<String, Object>) output.getRows().get(0);
        List<String> keys = new ArrayList<>(doc.keySet());
        System.out.println(keys);
        assertThat(keys, contains("_id", "a", "z", "m"));
    }
}
