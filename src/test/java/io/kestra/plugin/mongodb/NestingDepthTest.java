package io.kestra.plugin.mongodb;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;

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
import static org.hamcrest.Matchers.not;
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

    @Test
    void bulkCountsDepthAfterARegexLiteralHoldingClosers() throws Exception {
        var uri = store("{\"insertOne\": {\"document\": {\"r\": /" + "}".repeat(TOO_DEEP) + "/, \"d\": " + nested(true, TOO_DEEP, "1") + "}}}\n");

        var e = assertThrows(Exception.class, () -> bulk(uri).run(runContextFactory.of()));

        assertThat(e.getMessage(), containsString("100 levels"));
    }

    @Test
    void bulkCountsDepthAfterASingleQuotedStringHoldingClosers() throws Exception {
        var uri = store("{\"insertOne\": {\"document\": {\"s\": '" + "]".repeat(TOO_DEEP) + "', \"d\": " + nested(false, TOO_DEEP, "1") + "}}}\n");

        var e = assertThrows(Exception.class, () -> bulk(uri).run(runContextFactory.of()));

        assertThat(e.getMessage(), containsString("100 levels"));
    }

    @Test
    void loadAcceptsExactlyTheLimit() throws Exception {
        // the record is level 1, so a field holding 99 levels reaches level 100
        var output = load(store("{\"doc\": " + nested(true, 99, "1") + "}\n{\"doc\": " + nested(false, 99, "1") + "}\n")).run(runContextFactory.of());

        assertThat(output.getSize(), is(2L));
    }

    @Test
    void loadRejectsOneLevelPastTheLimit() throws Exception {
        for (var objects : new boolean[]{true, false}) {
            var uri = store("{\"doc\": " + nested(objects, 100, "1") + "}\n");

            var e = assertThrows(Exception.class, () -> load(uri).run(runContextFactory.of()));

            assertThat(e.getMessage(), containsString("Record 1"));
            assertThat(e.getMessage(), containsString("100 levels"));
            assertThat(e.getMessage(), not(containsString("102")));
        }
    }

    @Test
    void loadAcceptsAnExtendedJsonWrapperAtLevel101() throws Exception {
        var uri = store("{\"doc\": " + nested(true, 99, "{\"$oid\": \"507f1f77bcf86cd799439011\"}") + "}\n");

        var output = load(uri).run(runContextFactory.of());

        assertThat(output.getSize(), is(1L));
    }

    @Test
    void bulkAcceptsExactlyTheLimitAndRejectsOneLevelPastIt() throws Exception {
        var accepted = bulk(store(
            "{\"insertOne\": {\"document\": {\"d\": " + nested(true, 99, "1") + "}}}\n"
                + "{\"replaceOne\": {\"filter\": {\"_id\": 1}, \"replacement\": {\"d\": " + nested(false, 99, "1") + "}, \"upsert\": true}}\n"
        )).run(runContextFactory.of());
        assertThat(accepted.getSize(), is(2L));

        var e = assertThrows(Exception.class, () -> bulk(store("{\"insertOne\": {\"document\": {\"d\": " + nested(true, 100, "1") + "}}}\n")).run(runContextFactory.of()));
        assertThat(e.getMessage(), containsString("Invalid bulk request on line 1"));
        assertThat(e.getMessage(), containsString("more than 100 levels"));
        assertThat(e.getMessage(), not(containsString("102")));
    }

    @Test
    void bulkShorthandInsertOneIsMeasuredWithoutTheOperationLevel() throws Exception {
        var accepted = bulk(store("{\"insertOne\": {\"d\": " + nested(true, 99, "1") + "}}\n")).run(runContextFactory.of());
        assertThat(accepted.getSize(), is(1L));

        var e = assertThrows(Exception.class, () -> bulk(store("{\"insertOne\": {\"d\": " + nested(true, 100, "1") + "}}\n")).run(runContextFactory.of()));
        assertThat(e.getMessage(), containsString("more than 100 levels"));
    }

    @Test
    void bulkUpdateOperatorLevelIsNotCounted() throws Exception {
        var output = bulk(store(
            "{\"updateOne\": {\"filter\": {\"_id\": 1}, \"update\": {\"$set\": {\"d\": " + nested(true, 99, "1") + "}}, \"upsert\": true}}\n"
        )).run(runContextFactory.of());
        assertThat(output.getSize(), is(1L));

        var e = assertThrows(Exception.class, () -> bulk(store(
            "{\"updateOne\": {\"filter\": {\"_id\": 1}, \"update\": {\"$set\": {\"d\": " + nested(true, 100, "1") + "}}}}\n"
        )).run(runContextFactory.of()));
        assertThat(e.getMessage(), containsString("more than 100 levels"));
    }

    @Test
    void bulkRejectsADeepFilter() throws Exception {
        var e = assertThrows(Exception.class, () -> bulk(store(
            "{\"deleteMany\": {\"filter\": {\"d\": " + nested(true, 100, "1") + "}}}\n"
        )).run(runContextFactory.of()));

        assertThat(e.getMessage(), containsString("more than 100 levels"));
    }

    @Test
    void insertOneRejectsADeepDocument() {
        var insert = InsertOne.builder()
            .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(database()))
            .collection(Property.ofValue("nesting"))
            .document(nested(true, TOO_DEEP, "1"))
            .build();

        var e = assertThrows(Exception.class, () -> insert.run(runContextFactory.of()));

        assertThat(e.getMessage(), containsString("more than 100 levels"));
    }

    @Test
    void findRejectsADeepFilter() {
        for (var filter : new Object[]{nested(true, TOO_DEEP, "1"), deepMap(101)}) {
            var find = find(filter);

            var e = assertThrows(Exception.class, () -> find.run(runContextFactory.of()));

            assertThat(e.getMessage(), containsString("more than 100 levels"));
        }
    }

    private Find find(Object filter) {
        return Find.builder()
            .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(database()))
            .collection(Property.ofValue("nesting"))
            .filter(filter)
            .build();
    }

    private static Map<String, Object> deepMap(int depth) {
        Map<String, Object> map = Map.of("a", 1);
        for (var i = 1; i < depth; i++) {
            map = Map.of("a", map);
        }
        return map;
    }

    @Test
    void parseCountsExactlyTheDocumentLevels() {
        assertThat(MongoDbService.parse(nested(true, 100, "1")).size(), is(1));
        assertThrows(IllegalArgumentException.class, () -> MongoDbService.parse(nested(true, 101, "1")));
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
