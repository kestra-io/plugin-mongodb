package io.kestra.plugin.mongodb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.function.BooleanSupplier;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;
import com.mongodb.client.model.Filters;

import io.kestra.core.models.property.Property;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class UnacknowledgedWriteTest extends MongoDbContainer {

    @Inject
    private StorageInterface storageInterface;

    @Test
    void insertOneUnacknowledged() throws Exception {
        var database = database();

        var output = InsertOne.builder()
            .connection(unacknowledgedConnection())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("unack_insert"))
            .document(ImmutableMap.of("name", "John Doe"))
            .build()
            .run(runContextFactory.of());

        assertThat(output.getWasAcknowledged(), is(false));
        awaitCount(database, "unack_insert", Filters.eq("name", "John Doe"), 1);
        try (var client = getMongoClient()) {
            var stored = client.getDatabase(database).getCollection("unack_insert").find().first();
            assertThat(output.getInsertedId(), is(stored.getObjectId("_id").toHexString()));
        }
    }

    @Test
    void updateUnacknowledged() throws Exception {
        var database = database();
        try (var client = getMongoClient()) {
            client.getDatabase(database).getCollection("unack_update").insertOne(new Document("name", "John Doe").append("visits", 1));
        }

        var output = Update.builder()
            .connection(unacknowledgedConnection())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("unack_update"))
            .document("{\"$inc\": {\"visits\": 1}}")
            .filter(ImmutableMap.of("name", "John Doe"))
            .build()
            .run(runContextFactory.of());

        assertThat(output.getWasAcknowledged(), is(false));
        assertThat(output.getUpsertedId(), nullValue());
        assertThat(output.getMatchedCount(), nullValue());
        assertThat(output.getModifiedCount(), nullValue());
        awaitCount(database, "unack_update", Filters.eq("visits", 2), 1);
    }

    @Test
    void deleteUnacknowledged() throws Exception {
        var database = database();
        try (var client = getMongoClient()) {
            client.getDatabase(database).getCollection("unack_delete").insertOne(new Document("name", "John Doe"));
        }

        var output = Delete.builder()
            .connection(unacknowledgedConnection())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("unack_delete"))
            .filter(ImmutableMap.of("name", "John Doe"))
            .build()
            .run(runContextFactory.of());

        assertThat(output.getWasAcknowledged(), is(false));
        assertThat(output.getDeletedCount(), nullValue());
        awaitCount(database, "unack_delete", Filters.empty(), 0);
    }

    @Test
    void loadUnacknowledged() throws Exception {
        var database = database();
        var file = File.createTempFile("unacknowledged_load_", ".ion");
        try (var output = new FileOutputStream(file)) {
            for (var i = 0; i < 5; i++) {
                FileSerde.write(output, ImmutableMap.of("name", "john-%d".formatted(i)));
            }
        }

        // 5 records in chunks of 2: before the fix the task failed after the first chunk and never sent the rest
        var output = Load.builder()
            .connection(unacknowledgedConnection())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("unack_load"))
            .from(Property.ofValue(upload(file).toString()))
            .chunk(Property.ofValue(2))
            .build()
            .run(runContextFactory.of());

        assertThat(output.getSize(), is(5L));
        assertThat(output.getWasAcknowledged(), is(false));
        assertThat(output.getInsertedCount(), is(0));
        awaitCount(database, "unack_load", Filters.empty(), 5);
    }

    @Test
    void bulkUnacknowledged() throws Exception {
        var database = database();
        var file = File.createTempFile("unacknowledged_bulk_", ".ion");
        try (var output = new FileOutputStream(file)) {
            for (var i = 0; i < 5; i++) {
                output.write("{ insertOne: { \"document\": { \"name\": \"john-%d\" } } }\n".formatted(i).getBytes(StandardCharsets.UTF_8));
            }
        }

        var output = Bulk.builder()
            .connection(unacknowledgedConnection())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("unack_bulk"))
            .from(Property.ofValue(upload(file).toString()))
            .chunk(Property.ofValue(2))
            .build()
            .run(runContextFactory.of());

        assertThat(output.getSize(), is(5L));
        assertThat(output.getWasAcknowledged(), is(false));
        assertThat(output.getInsertedCount(), is(0));
        awaitCount(database, "unack_bulk", Filters.empty(), 5);
    }

    private static String database() {
        return "ut_%s".formatted(IdUtils.create().toLowerCase(Locale.ROOT));
    }

    private static MongoDbConnection unacknowledgedConnection() {
        var uri = "%s%sw=0".formatted(connectionUri, connectionUri.contains("?") ? "&" : "?");
        return MongoDbConnection.builder().uri(Property.ofValue(uri)).build();
    }

    private URI upload(File file) throws Exception {
        try (var input = new FileInputStream(file)) {
            return storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/%s.ion".formatted(IdUtils.create())), input);
        }
    }

    // a w=0 write returns before the server applies it, so the check waits for it instead of reading once
    private void awaitCount(String database, String collection, Bson filter, long expected) throws Exception {
        try (var client = getMongoClient()) {
            BooleanSupplier reached = () -> client.getDatabase(database).getCollection(collection).countDocuments(filter) == expected;
            Await.until(reached, Duration.ofMillis(20), Duration.ofSeconds(10));
        }
    }
}
