package io.kestra.plugin.mongodb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.IntStream;

import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ValidationOptions;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTaskException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class LoadTest extends MongoDbContainer {

    @Inject
    private StorageInterface storageInterface;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of();
        String database = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        for (int i = 0; i < 100; i++) {
            FileSerde.write(
                output, ImmutableMap.of(
                    "id", new ObjectId().toString(),
                    "name", "john"
                )
            );
        }
        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Load put = Load.builder()
            .connection(
                MongoDbConnection.builder()
                    .uri(Property.ofValue(connectionUri))
                    .build()
            )
            .database(Property.ofValue(database))
            .collection(Property.ofValue("load"))
            .from(Property.ofValue(uri.toString()))
            .chunk(Property.ofValue(10))
            .build();

        Load.Output runOutput = put.run(runContext);

        assertThat(runOutput.getSize(), is(100L));
        assertThat(runOutput.getInsertedCount(), is(100));
        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("requests.count")).findFirst().orElseThrow().getValue(), is(10D));
        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("records")).findFirst().orElseThrow().getValue(), is(100D));
    }

    @Test
    void runWithIdKey() throws Exception {
        RunContext runContext = runContextFactory.of();
        String database = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_idkey_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        ObjectId expectedId = new ObjectId();
        FileSerde.write(
            output, ImmutableMap.of(
                "id", expectedId.toString(),
                "name", "john"
            )
        );
        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Load put = Load.builder()
            .connection(
                MongoDbConnection.builder()
                    .uri(Property.ofValue(connectionUri))
                    .build()
            )
            .database(Property.ofValue(database))
            .collection(Property.ofValue("load_idkey"))
            .from(Property.ofValue(uri.toString()))
            .idKey(Property.ofValue("id"))
            .build();

        Load.Output runOutput = put.run(runContext);

        assertThat(runOutput.getInsertedCount(), is(1));

        try (MongoClient client = getMongoClient()) {
            Document stored = client.getDatabase(database)
                .getCollection("load_idkey", Document.class)
                .find()
                .first();

            assertThat(stored, is(org.hamcrest.Matchers.notNullValue()));
            assertThat(stored.get("_id"), is(expectedId));
            assertThat(stored.containsKey("id"), is(false));
            assertThat(stored.getString("name"), is("john"));
        }
    }

    @Test
    void runWithExtendedJson() throws Exception {
        RunContext runContext = runContextFactory.of();
        String database = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_extended_json_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        ObjectId expectedId = new ObjectId();
        FileSerde.write(
            output, ImmutableMap.of(
                "id", expectedId.toString(),
                "createdAt", ImmutableMap.of("$date", "2024-01-01T00:00:00Z"),
                "amount", ImmutableMap.of("$numberDecimal", "12.34"),
                "externalId", ImmutableMap.of("$oid", "60930c39a982931c20ef6cd6"),
                "nested", ImmutableMap.of(
                    "count", ImmutableMap.of("$numberLong", "123"),
                    "ids", List.of(ImmutableMap.of("$oid", "60930c39a982931c20ef6cd6"))
                )
            )
        );
        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Load put = Load.builder()
            .connection(
                MongoDbConnection.builder()
                    .uri(Property.ofValue(connectionUri))
                    .build()
            )
            .database(Property.ofValue(database))
            .collection(Property.ofValue("load_extended_json"))
            .from(Property.ofValue(uri.toString()))
            .idKey(Property.ofValue("id"))
            .build();

        Load.Output runOutput = put.run(runContext);

        assertThat(runOutput.getInsertedCount(), is(1));

        try (MongoClient client = getMongoClient()) {
            BsonDocument stored = client.getDatabase(database)
                .getCollection("load_extended_json", BsonDocument.class)
                .find()
                .first();

            assertThat(stored, is(org.hamcrest.Matchers.notNullValue()));
            assertThat(stored.getObjectId("_id").getValue(), is(expectedId));
            assertThat(stored.containsKey("id"), is(false));
            assertThat(stored.get("createdAt").getBsonType(), is(BsonType.DATE_TIME));
            assertThat(stored.get("amount").getBsonType(), is(BsonType.DECIMAL128));
            assertThat(stored.get("externalId").getBsonType(), is(BsonType.OBJECT_ID));
            assertThat(stored.getDocument("nested").get("count").getBsonType(), is(BsonType.INT64));
            assertThat(stored.getDocument("nested").getArray("ids").get(0).getBsonType(), is(BsonType.OBJECT_ID));
        }
    }

    @Test
    void failedWriteReportsTheRecordAndWhatWasApplied() throws Exception {
        RunContext runContext = runContextFactory.of();
        String database = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);
        try (MongoClient client = getMongoClient()) {
            // records 6 and above fail validation
            client.getDatabase(database).createCollection(
                "validated",
                new CreateCollectionOptions().validationOptions(new ValidationOptions().validator(Filters.lt("n", 6)))
            );
        }

        URI uri = put(IntStream.rangeClosed(1, 10).<Map<String, Object>>mapToObj(n -> Map.of("n", n)).toList());
        Load load = load(database, "validated", uri).chunk(Property.ofValue(4)).build();

        var e = assertThrows(RunnableTaskException.class, () -> load.run(runContext));

        assertThat(e.getMessage(), is("Record 6 failed (5 records applied before it): Document failed validation"));
        assertThat(e.getCause(), instanceOf(MongoBulkWriteException.class));
        var output = (Load.Output) e.getOutput();
        assertThat(output.getSize(), is(5L));
        assertThat(output.getInsertedCount(), is(5));
        assertThat(metric(runContext, "records"), is(5D));
        assertThat(metric(runContext, "requests.count"), is(2D));
        assertThat(count(database, "validated"), is(5L));
    }

    @Test
    void retryWithIdKeyFailsOnTheFirstRecordAlreadyWritten() throws Exception {
        String database = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);
        URI uri = put(
            List.of(
                Map.of("id", new ObjectId().toString()),
                Map.of("id", new ObjectId().toString())
            )
        );
        Load load = load(database, "retried", uri).idKey(Property.ofValue("id")).build();
        load.run(runContextFactory.of());

        var e = assertThrows(RunnableTaskException.class, () -> load.run(runContextFactory.of()));

        assertThat(e.getMessage(), startsWith("Record 1 failed (0 records applied before it): E11000 duplicate key error"));
        assertThat(((Load.Output) e.getOutput()).getInsertedCount(), is(0));
        assertThat(count(database, "retried"), is(2L));
    }

    private URI put(List<Map<String, Object>> records) throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            for (var record : records) {
                FileSerde.write(output, record);
            }
        }
        return storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));
    }

    private static Load.LoadBuilder<?, ?> load(String database, String collection, URI uri) {
        return Load.builder()
            .connection(
                MongoDbConnection.builder()
                    .uri(Property.ofValue(connectionUri))
                    .build()
            )
            .database(Property.ofValue(database))
            .collection(Property.ofValue(collection))
            .from(Property.ofValue(uri.toString()));
    }

    private static Double metric(RunContext runContext, String name) {
        return (Double) runContext.metrics().stream().filter(e -> e.getName().equals(name)).findFirst().orElseThrow().getValue();
    }

    private static long count(String database, String collection) {
        try (MongoClient client = getMongoClient()) {
            return client.getDatabase(database).getCollection(collection).countDocuments();
        }
    }

    @Test
    void loadWithOffsetDateTimeAndBigInteger() throws Exception {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            FileSerde.write(output, Map.of(
                "k", "offset_date",
                "ts", java.time.OffsetDateTime.parse("2024-01-01T10:00:00+02:00"),
                "zoned", java.time.ZonedDateTime.parse("2024-01-01T12:00:00Z"),
                "big", new java.math.BigInteger("12345678901234567890")
            ));
        }

        URI uri = storageInterface.put(TenantService.DEFAULT_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        String database = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);
        Load task = Load.builder()
            .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("load_dates"))
            .from(Property.ofValue(uri.toString()))
            .build();

        RunContext runContext = runContextFactory.of();
        Load.Output runOutput = task.run(runContext);

        assertThat(runOutput.getSize(), is(1L));
        assertThat(runOutput.getInsertedCount(), is(1L));
    }
}
