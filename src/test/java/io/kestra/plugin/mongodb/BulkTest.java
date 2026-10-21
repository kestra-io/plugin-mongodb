package io.kestra.plugin.mongodb;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTaskException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class BulkTest extends MongoDbContainer {

    @Inject
    private StorageInterface storageInterface;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of();
        String database = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            output.write(
                ("{ insertOne: { \"document\": { \"_id\" : 1, \"char\" : \"Brisbane\", \"class\" : \"monk\", \"lvl\" : 4, \"skills\" : [{ \"name\": \"sword\", \"level\": 2 }, {\"name\": \"magic\", \"level\": 1}] } } }\n")
                    .getBytes(StandardCharsets.UTF_8)
            );
            output.write(
                ("{ insertOne: { \"document\": { \"_id\" : 2, \"char\" : \"Eldon\", \"class\" : \"alchemist\", \"lvl\" : 3, \"skills\" : [{ \"name\": \"alchemy\", \"level\": 3 }, {\"name\": \"potion\", \"level\": 2}] } } }\n")
                    .getBytes(StandardCharsets.UTF_8)
            );
            output.write(
                ("{ insertOne: { \"document\": { \"_id\" : 3, \"char\" : \"Meldane\", \"class\" : \"ranger\", \"lvl\" : 3, \"skills\" : [{ \"name\": \"bow\", \"level\": 3 }, {\"name\": \"tracking\", \"level\": 2}] } } }\n")
                    .getBytes(StandardCharsets.UTF_8)
            );
            output.write(
                ("{ insertOne: { \"document\": { \"_id\": 4, \"char\": \"Dithras\", \"class\": \"barbarian\", \"lvl\": 1, \"skills\" : [{ \"name\": \"axe\", \"level\": 3 }, {\"name\": \"rage\", \"level\": 2}] } } }\n")
                    .getBytes(StandardCharsets.UTF_8)
            );
            output.write(
                ("{ insertOne: { \"document\": { \"_id\": 5, \"char\": \"Taeln\", \"class\": \"fighter\", \"lvl\": 1, \"skills\" : [{ \"name\": \"sword\", \"level\": 3 }, {\"name\": \"shield\", \"level\": 2}] } } }\n")
                    .getBytes(StandardCharsets.UTF_8)
            );

            output.write(
                ("{ updateMany : {\"filter\" : { \"lvl\" : 1 }, \"update\" : { $set : { \"skills.$[elem].level\" : 4 } }, \"arrayFilters\" : [ { \"elem.level\" : { \"$eq\" : 2 } } ] } }\n")
                    .getBytes(StandardCharsets.UTF_8)
            );

            output.write(
                ("{ updateOne : {\"filter\" : { \"char\" : \"Eldon\" },\"update\" : { $set : { \"status\" : \"Critical Injury\" } }, \"upsert\": true } }\n").getBytes(StandardCharsets.UTF_8)
            );

            output.write(("{ deleteOne : { \"filter\" : { \"char\" : \"Brisbane\"} } }\n").getBytes(StandardCharsets.UTF_8));

            output.write(
                ("{ replaceOne : {\"filter\" : { \"char\" : \"Meldane\" },\"replacement\" : { \"char\" : \"Tanys\", \"class\" : \"oracle\", \"lvl\": 4 }, \"collation\": { \"locale\": \"en\", \"strength\": 2 } } }\n")
                    .getBytes(StandardCharsets.UTF_8)
            );
        }

        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Bulk put = Bulk.builder()
            .connection(
                MongoDbConnection.builder()
                    .uri(Property.ofValue(connectionUri))
                    .build()
            )
            .database(Property.ofValue(database))
            .collection(Property.ofValue("bulk"))
            .from(Property.ofValue(uri.toString()))
            .chunk(Property.ofValue(10))
            .build();

        Bulk.Output runOutput = put.run(runContext);

        assertThat(runOutput.getSize(), is(9L));
        assertThat(runOutput.getInsertedCount(), is(5));
        assertThat(runOutput.getMatchedCount(), is(4));
        assertThat(runOutput.getModifiedCount(), is(4));
        assertThat(runOutput.getDeletedCount(), is(1));
        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("requests.count")).findFirst().orElseThrow().getValue(), is(1D));
        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("records")).findFirst().orElseThrow().getValue(), is(9D));
    }

    @Test
    void runWithStandardAndFlatInsertOne() throws Exception {
        RunContext runContext = runContextFactory.of();
        String database = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_insert_", ".trs");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            output.write(
                ("{ \"insertOne\": { \"document\": { \"_id\": 42, \"name\": \"Diya\", \"kind\": \"standard\" } } }\n")
                    .getBytes(StandardCharsets.UTF_8)
            );
            output.write(
                ("{ \"insertOne\": { \"_id\": 43, \"name\": \"Legacy\", \"kind\": \"flat\" } }\n")
                    .getBytes(StandardCharsets.UTF_8)
            );
            output.write(
                ("{ \"insertOne\": { \"_id\": 44, \"document\": { \"title\": \"Report\" }, \"kind\": \"flat\" } }\n")
                    .getBytes(StandardCharsets.UTF_8)
            );
            output.write(
                ("{ \"updateOne\": { \"filter\": { \"_id\": 42 }, \"update\": { \"$set\": { \"updated\": true } } } }\n")
                    .getBytes(StandardCharsets.UTF_8)
            );
        }

        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Bulk put = Bulk.builder()
            .connection(
                MongoDbConnection.builder()
                    .uri(Property.ofValue(connectionUri))
                    .build()
            )
            .database(Property.ofValue(database))
            .collection(Property.ofValue("bulk_insert"))
            .from(Property.ofValue(uri.toString()))
            .build();

        put.run(runContext);

        try (MongoClient client = getMongoClient()) {
            MongoCollection<Document> collection = client.getDatabase(database).getCollection("bulk_insert", Document.class);

            assertThat(
                collection.find(new Document("_id", 42)).first(),
                is(new Document("_id", 42).append("name", "Diya").append("kind", "standard").append("updated", true))
            );
            assertThat(
                collection.find(new Document("_id", 43)).first(),
                is(new Document("_id", 43).append("name", "Legacy").append("kind", "flat"))
            );
            assertThat(
                collection.find(new Document("_id", 44)).first(),
                is(new Document("_id", 44).append("document", new Document("title", "Report")).append("kind", "flat"))
            );
        }
    }

    @Test
    void singleObjectDocumentFieldIsReadAsBulkWriteEnvelope() throws Exception {
        String database = newDatabase();

        // documented rule: a body whose only field is an object named "document" is the envelope, even when empty
        runBulk(
            database, "envelope_rule",
            "{\"insertOne\": {\"document\": {\"title\": \"Report\"}}}",
            "{\"insertOne\": {\"document\": {}}}"
        );

        List<Document> stored = findAll(database, "envelope_rule");
        assertThat(stored, hasSize(2));
        for (Document document : stored) {
            assertThat(document.get("_id"), instanceOf(ObjectId.class));
            assertThat(document.containsKey("document"), is(false));
        }
        assertThat(stored.stream().map(document -> document.get("title")).toList(), containsInAnyOrder("Report", null));
    }

    @Test
    void standardInsertOneUnwrapsOnlyTheOuterEnvelope() throws Exception {
        String database = newDatabase();

        runBulk(database, "unwrap_once", "{\"insertOne\": {\"document\": {\"_id\": 1, \"document\": {\"title\": \"Report\"}}}}");

        assertThat(findAll(database, "unwrap_once"), is(List.of(new Document("_id", 1).append("document", new Document("title", "Report")))));
    }

    @Test
    void insertOneWithNonObjectDocumentFieldStaysFlat() throws Exception {
        String database = newDatabase();

        runBulk(
            database, "non_object",
            "{\"insertOne\": {\"document\": \"plain text\"}}",
            "{\"insertOne\": {\"document\": [{\"a\": 1}]}}",
            "{\"insertOne\": {\"document\": null}}"
        );

        List<Document> stored = findAll(database, "non_object");
        assertThat(stored, hasSize(3));
        for (Document document : stored) {
            assertThat(document.keySet(), containsInAnyOrder("_id", "document"));
        }
        assertThat(
            stored.stream().map(document -> document.get("document")).toList(),
            containsInAnyOrder("plain text", List.of(new Document("a", 1)), null)
        );
    }

    @Test
    void insertOneWithNestedIdWrittenLast() throws Exception {
        var database = newDatabase();

        // a nested _id as the last field name used to fail the whole chunk, the valid first record included
        var output = runBulk(
            database, "nested_id",
            "{\"insertOne\": {\"document\": {\"k\": \"first\"}}}",
            "{\"insertOne\": {\"document\": {\"k\": \"last\", \"customer\": {\"_id\": 42}}}}",
            "{\"insertOne\": {\"k\": \"flat\", \"items\": [{\"q\": 1, \"_id\": 7}]}}"
        );

        assertThat(output.getInsertedCount(), is(3));
        var stored = findAll(database, "nested_id");
        for (var document : stored) {
            assertThat(document.remove("_id"), instanceOf(ObjectId.class));
        }
        assertThat(
            stored,
            containsInAnyOrder(
                new Document("k", "first"),
                new Document("k", "last").append("customer", new Document("_id", 42)),
                new Document("k", "flat").append("items", List.of(new Document("q", 1).append("_id", 7)))
            )
        );
    }

    @Test
    void upsertsAreCounted() throws Exception {
        var database = newDatabase();
        try (MongoClient client = getMongoClient()) {
            client.getDatabase(database).getCollection("upserts").insertOne(new Document("_id", 3).append("c", 0));
        }

        var output = runBulk(
            database, "upserts",
            "{\"updateOne\": {\"filter\": {\"_id\": 1}, \"update\": {\"$set\": {\"a\": 1}}, \"upsert\": true}}",
            "{\"replaceOne\": {\"filter\": {\"_id\": 2}, \"replacement\": {\"b\": 2}, \"upsert\": true}}",
            "{\"updateOne\": {\"filter\": {\"_id\": 3}, \"update\": {\"$set\": {\"c\": 3}}, \"upsert\": true}}"
        );

        assertThat(output.getUpsertedCount(), is(2));
        assertThat(output.getMatchedCount(), is(1));
        assertThat(output.getModifiedCount(), is(1));
        assertThat(output.getInsertedCount(), is(0));
        assertThat(findAll(database, "upserts"), hasSize(3));
    }

    @Test
    void readerStopsOnceAWriteFails() throws Exception {
        var linesRead = new AtomicInteger();
        var input = new BufferedReader(new StringReader("{\"insertOne\": {\"document\": {\"a\": 1}}}\n".repeat(1_000))) {
            @Override
            public String readLine() throws IOException {
                linesRead.incrementAndGet();
                return super.readLine();
            }
        };

        // the shape of AbstractLoad.run with a chunk of 2, where the first bulkWrite fails
        var run = Flux.create(Bulk.builder().build().ndJSonReader(input), FluxSink.OverflowStrategy.BUFFER)
            .buffer(2)
            .map(chunk ->
            {
                throw new IllegalStateException("bulkWrite failed");
            });

        assertThrows(IllegalStateException.class, () -> run.count().block());
        assertThat("only the failed chunk is read", linesRead.get(), is(2));
    }

    @Test
    void failedOperationReportsItsNumberAndWhatWasApplied() throws Exception {
        var database = newDatabase();
        var runContext = runContextFactory.of();
        var bulk = Bulk.builder()
            .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("failing"))
            .from(Property.ofValue(store(
                "{\"updateOne\": {\"filter\": {\"_id\": 1}, \"update\": {\"$set\": {\"a\": 1}}, \"upsert\": true}}",
                "{\"updateOne\": {\"filter\": {\"_id\": 2}, \"update\": {\"$set\": {\"a\": 2}}, \"upsert\": true}}",
                "{\"insertOne\": {\"document\": {\"_id\": 3}}}",
                "{\"insertOne\": {\"document\": {\"_id\": 1}}}",
                "{\"insertOne\": {\"document\": {\"_id\": 5}}}"
            ).toString()))
            .chunk(Property.ofValue(2))
            .build();

        var e = assertThrows(RunnableTaskException.class, () -> bulk.run(runContext));

        assertThat(e.getMessage(), startsWith("Record 4 failed (3 records applied before it): E11000 duplicate key error"));
        assertThat(e.getCause(), instanceOf(MongoBulkWriteException.class));
        var output = (Bulk.Output) e.getOutput();
        assertThat(output.getSize(), is(3L));
        assertThat(output.getUpsertedCount(), is(2));
        assertThat(output.getInsertedCount(), is(1));
        assertThat(metric(runContext, "records"), is(3D));
        assertThat(metric(runContext, "requests.count"), is(2D));
        assertThat(findAll(database, "failing").stream().map(document -> document.get("_id")).toList(), containsInAnyOrder(1, 2, 3));
    }

    @Test
    void writeWithoutAResultLeavesItsChunkUnknown() throws Exception {
        var database = newDatabase();
        var runContext = runContextFactory.of();
        var bulk = Bulk.builder()
            .connection(SecondWriteFailsConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("unknown"))
            .from(Property.ofValue(store(
                "{\"insertOne\": {\"document\": {\"_id\": 1}}}",
                "{\"insertOne\": {\"document\": {\"_id\": 2}}}",
                "{\"insertOne\": {\"document\": {\"_id\": 3}}}",
                "{\"insertOne\": {\"document\": {\"_id\": 4}}}",
                "{\"insertOne\": {\"document\": {\"_id\": 5}}}"
            ).toString()))
            .chunk(Property.ofValue(2))
            .build();

        var e = assertThrows(RunnableTaskException.class, () -> bulk.run(runContext));

        assertThat(
            e.getMessage(),
            is("The chunk of records 3 to 4 failed and MongoDB returned no result, so it's unknown which of its records were applied (2 records applied before it): connection reset")
        );
        assertThat(e.getCause(), instanceOf(MongoSocketReadException.class));
        var output = (Bulk.Output) e.getOutput();
        assertThat(output.getSize(), is(2L));
        assertThat(output.getInsertedCount(), is(2));
        assertThat(metric(runContext, "records"), is(2D));
        assertThat(metric(runContext, "requests.count"), is(2D));
    }

    // fails the second bulkWrite with no write result; overriding Bulk would register a plugin
    @SuperBuilder
    private static class SecondWriteFailsConnection extends MongoDbConnection {
        @Override
        MongoClient client(RunContext runContext, CommandListener listener) throws IllegalVariableEvaluationException {
            return failSecondWrite(super.client(runContext, listener), MongoClient.class, new AtomicInteger());
        }

        // proxies the client, and the databases and collections it returns, down to bulkWrite
        @SuppressWarnings({"unchecked", "rawtypes"})
        private static <T> T failSecondWrite(T target, Class<T> type, AtomicInteger writes) {
            return type.cast(Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[] {type},
                (proxy, method, args) ->
                {
                    if (method.getName().equals("bulkWrite") && writes.incrementAndGet() == 2) {
                        throw new MongoSocketReadException("connection reset", new ServerAddress());
                    }

                    Object result;
                    try {
                        result = method.invoke(target, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }

                    if (result instanceof MongoDatabase database) {
                        return failSecondWrite(database, MongoDatabase.class, writes);
                    }
                    if (result instanceof MongoCollection collection) {
                        return failSecondWrite(collection, (Class) MongoCollection.class, writes);
                    }
                    return result;
                }
            ));
        }
    }

    private static String newDatabase() {
        return "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);
    }

    private Bulk.Output runBulk(String database, String collection, String... lines) throws Exception {
        URI uri = store(lines);

        return Bulk.builder()
            .connection(
                MongoDbConnection.builder()
                    .uri(Property.ofValue(connectionUri))
                    .build()
            )
            .database(Property.ofValue(database))
            .collection(Property.ofValue(collection))
            .from(Property.ofValue(uri.toString()))
            .build()
            .run(runContextFactory.of());
    }

    private URI store(String... lines) throws Exception {
        return storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new ByteArrayInputStream((String.join("\n", lines) + "\n").getBytes(StandardCharsets.UTF_8))
        );
    }

    private static Double metric(RunContext runContext, String name) {
        return (Double) runContext.metrics().stream().filter(e -> e.getName().equals(name)).findFirst().orElseThrow().getValue();
    }

    private static List<Document> findAll(String database, String collection) {
        try (MongoClient client = getMongoClient()) {
            return client.getDatabase(database).getCollection(collection, Document.class).find().into(new ArrayList<>());
        }
    }
}
