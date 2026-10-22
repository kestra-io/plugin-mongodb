package io.kestra.plugin.mongodb;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest(startRunner = false, startWorker = false)
class MongoDbNullValueTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void nullFilterEntryStaysInTheDocument() throws Exception {
        var filter = new LinkedHashMap<String, Object>();
        filter.put("deletedAt", null);
        filter.put("name", "kept");

        var document = MongoDbService.toDocument(this.runContext(), filter);

        assertThat(document.containsKey("deletedAt"), is(true));
        assertThat(document.get("deletedAt").isNull(), is(true));
        assertThat(document.getString("name").getValue(), is("kept"));
    }

    @Test
    void nullSetEntryStaysInTheDocument() throws Exception {
        var set = new LinkedHashMap<String, Object>();
        set.put("k", null);
        var update = new LinkedHashMap<String, Object>();
        update.put("$set", set);

        var document = MongoDbService.toDocument(this.runContext(), update);

        assertThat(document.getDocument("$set").get("k").isNull(), is(true));
    }

    @Test
    void nullInsideAnArrayStaysInTheDocument() throws Exception {
        var filter = new LinkedHashMap<String, Object>();
        filter.put("k", Map.of("$in", Arrays.asList("x", null)));

        var document = MongoDbService.toDocument(this.runContext(), filter);

        assertThat(document.getDocument("k").getArray("$in").get(1).isNull(), is(true));
    }

    @Test
    void stringFormStillParsesANull() throws Exception {
        var document = MongoDbService.toDocument(this.runContext(), "{\"k\": null}");

        assertThat(document.get("k").isNull(), is(true));
    }

    @Test
    void queueSerializationKeepsANullFilter() throws Exception {
        var filter = new LinkedHashMap<String, Object>();
        filter.put("deletedAt", null);
        var built = Delete.builder()
            .id(IdUtils.create())
            .type(Delete.class.getName())
            .filter(filter)
            .build();
        var task = JacksonMapper.ofJson().readValue(JacksonMapper.ofJson().writeValueAsString(built), Delete.class);

        assertNullEntry(task.getFilter(), "deletedAt");
        var document = MongoDbService.toDocument(this.runContext(), task.getFilter());
        assertThat(document.get("deletedAt").isNull(), is(true));
    }

    @Test
    void queueSerializationKeepsANullDocumentField() throws Exception {
        var set = new LinkedHashMap<String, Object>();
        set.put("k", null);
        var updateDocument = new LinkedHashMap<String, Object>();
        updateDocument.put("$set", set);
        var update = JacksonMapper.ofJson().readValue(
            JacksonMapper.ofJson().writeValueAsString(Update.builder()
                .id(IdUtils.create())
                .type(Update.class.getName())
                .filter(Map.of("_id", 1))
                .document(updateDocument)
                .build()),
            Update.class
        );
        @SuppressWarnings("unchecked")
        var restoredSet = (Map<String, Object>) ((Map<String, Object>) update.getDocument()).get("$set");
        assertThat(restoredSet.containsKey("k"), is(true));
        assertThat(restoredSet.get("k"), is(nullValue()));

        var inserted = new LinkedHashMap<String, Object>();
        inserted.put("_id", 10);
        inserted.put("k", null);
        var insert = JacksonMapper.ofJson().readValue(
            JacksonMapper.ofJson().writeValueAsString(InsertOne.builder()
                .id(IdUtils.create())
                .type(InsertOne.class.getName())
                .document(inserted)
                .build()),
            InsertOne.class
        );
        assertNullEntry(insert.getDocument(), "k");
    }

    @Test
    void deleteManyWithEmptyMapFilterFails() {
        var task = Delete.builder()
            .id(IdUtils.create())
            .type(Delete.class.getName())
            .operation(Property.ofValue(Delete.Operation.DELETE_MANY))
            .filter(Map.of())
            .build();

        var error = assertThrows(IllegalArgumentException.class, () -> task.run(this.runContext()));
        assertThat(error.getMessage(), containsString("`filter` must not be an empty map with DELETE_MANY"));
    }

    @Test
    void updateWithEmptySetFails() {
        var task = Update.builder()
            .id(IdUtils.create())
            .type(Update.class.getName())
            .filter(Map.of("_id", 1))
            .document(Map.of("$set", Map.of()))
            .build();

        var error = assertThrows(IllegalArgumentException.class, () -> task.run(this.runContext()));
        assertThat(error.getMessage(), containsString("`$set` must not be empty"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void mapFormPipelineStillRenders() throws Exception {
        var task = Aggregate.builder()
            .pipeline(Property.ofValue(List.of(Map.of("$match", Map.of("status", "available")))))
            .build();
        var stages = task.renderedStages(this.runContext());

        var match = (Map<String, Object>) stages.getFirst().get("$match");
        assertThat(match.get("status"), is("available"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void aggregateJsonStringKeepsANull() throws Exception {
        var yaml = """
            id: aggregate
            type: io.kestra.plugin.mongodb.Aggregate
            pipeline: '[{"$match": {"k": null}}]'
            """;
        var task = JacksonMapper.ofYaml().readValue(yaml, Aggregate.class);
        var stages = task.renderedStages(this.runContext());
        var document = MongoDbService.toDocument(stages.getFirst());

        assertThat(document.getDocument("$match").get("k").isNull(), is(true));
    }

    @Test
    void aggregateDoesNotEvaluateExpressionsFoundInData() throws Exception {
        var task = Aggregate.builder()
            .pipeline(Property.ofValue(List.of(Map.of("$match", Map.of("label", "{{ payload }}")))))
            .build();
        var runContext = this.runContextFactory.of(Map.of("payload", "{{ 6 * 7 }}"));
        var document = MongoDbService.toDocument(task.renderedStages(runContext).getFirst());

        assertThat(document.getDocument("$match").getString("label").getValue(), is("{{ 6 * 7 }}"));
    }

    @SuppressWarnings("unchecked")
    private static void assertNullEntry(Object value, String key) {
        var map = (Map<String, Object>) value;
        assertThat(map.containsKey(key), is(true));
        assertThat(map.get(key), is(nullValue()));
    }

    private RunContext runContext() {
        return this.runContextFactory.of(Map.of());
    }
}
