package io.kestra.plugin.mongodb;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class NestingDepthTest extends MongoDbContainer {
    private static final int TOO_DEEP = 10_000;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void loadRejectsARecordNestedPastTheLimit() throws Exception {
        for (var objects : new boolean[]{true, false}) {
            var uri = store("{\"doc\": " + nested(objects, TOO_DEEP, "1") + "}\n");

            var e = assertThrows(Exception.class, () -> load(uri).run(runContextFactory.of()));

            assertThat(e.getMessage(), containsString("100 levels"));
        }
    }

    @Test
    void loadAcceptsARecordNestedWithinTheLimit() throws Exception {
        var uri = store("{\"id\": 1, \"doc\": " + nested(true, 50, "1") + "}\n{\"id\": 2, \"doc\": " + nested(false, 50, "1") + "}\n");

        var output = load(uri).run(runContextFactory.of());

        assertThat(output.getSize(), is(2L));
    }

    @Test
    void bulkRejectsALineNestedPastTheLimit() throws Exception {
        for (var objects : new boolean[]{true, false}) {
            var uri = store("{\"insertOne\": {\"document\": " + nested(objects, TOO_DEEP, "\"x\"") + "}}\n");

            var e = assertThrows(Exception.class, () -> bulk(uri).run(runContextFactory.of()));

            assertThat(e.getMessage(), containsString("Invalid bulk request on line 1"));
            assertThat(e.getMessage(), containsString("100 levels"));
        }
    }

    @Test
    void bulkAcceptsADocumentNestedWithinTheLimit() throws Exception {
        var uri = store("{\"insertOne\": {\"document\": " + nested(true, 50, "\"x\"") + "}}\n{\"insertOne\": {\"document\": " + nested(false, 50, "\"x\"") + "}}\n");

        var output = bulk(uri).run(runContextFactory.of());

        assertThat(output.getSize(), is(2L));
    }

    @Test
    void bulkIgnoresBracketsInsideStrings() throws Exception {
        var uri = store("{\"insertOne\": {\"document\": {\"text\": \"" + "[".repeat(TOO_DEEP) + "\\\"" + "{".repeat(TOO_DEEP) + "\"}}}\n");

        var output = bulk(uri).run(runContextFactory.of());

        assertThat(output.getSize(), is(1L));
    }

    private Load load(URI uri) {
        return Load.builder()
            .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(database()))
            .collection(Property.ofValue("nesting"))
            .from(Property.ofValue(uri.toString()))
            .build();
    }

    private Bulk bulk(URI uri) {
        return Bulk.builder()
            .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(database()))
            .collection(Property.ofValue("nesting"))
            .from(Property.ofValue(uri.toString()))
            .build();
    }

    private static String database() {
        return "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);
    }

    private static String nested(boolean objects, int depth, String leaf) {
        var open = objects ? "{\"a\":" : "[";
        var close = objects ? "}" : "]";
        return open.repeat(depth) + leaf + close.repeat(depth);
    }

    private URI store(String content) throws Exception {
        return storageInterface.put(
            TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"),
            new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))
        );
    }
}
