package io.kestra.plugin.mongodb;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

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

    private RunContext runContext() {
        return this.runContextFactory.of(Map.of());
    }
}
