package io.kestra.plugin.mongodb;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

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
                .uri("mongodb://root:example@localhost:27017/?authSource=admin")
                .build())
            .database(Property.of("samples"))
            .collection(Property.of("books"))
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
}
