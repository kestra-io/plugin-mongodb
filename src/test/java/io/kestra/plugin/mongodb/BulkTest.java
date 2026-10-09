package io.kestra.plugin.mongodb;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

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

    private static String newDatabase() {
        return "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);
    }

    private void runBulk(String database, String collection, String... lines) throws Exception {
        URI uri = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new ByteArrayInputStream((String.join("\n", lines) + "\n").getBytes(StandardCharsets.UTF_8))
        );

        Bulk.builder()
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

    private static List<Document> findAll(String database, String collection) {
        try (MongoClient client = getMongoClient()) {
            return client.getDatabase(database).getCollection(collection, Document.class).find().into(new ArrayList<>());
        }
    }
}
