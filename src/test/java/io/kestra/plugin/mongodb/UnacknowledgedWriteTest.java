package io.kestra.plugin.mongodb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Filters;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Covers unacknowledged writes ({@code w=0}): the driver throws on every id and
 * count getter of the write results, so the tasks must check {@code wasAcknowledged()}
 * first and succeed with empty ids/counts instead of failing.
 */
class UnacknowledgedWriteTest extends MongoDbContainer {

    @Inject
    private StorageInterface storageInterface;

    private static String unacknowledgedUri() {
        return connectionUri.contains("?") ? connectionUri + "&w=0" : connectionUri + "?w=0";
    }

    private static MongoDbConnection unacknowledgedConnection() {
        return MongoDbConnection.builder().uri(Property.ofValue(unacknowledgedUri())).build();
    }

    @Test
    void insertOneUnacknowledged() throws Exception {
        RunContext runContext = runContextFactory.of();
        String database = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        InsertOne insert = InsertOne.builder()
            .connection(unacknowledgedConnection())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("unack_insert"))
            .document(ImmutableMap.of("name", "John Doe"))
            .build();

        InsertOne.Output output = insert.run(runContext);

        assertThat(output.getWasAcknowledged(), is(false));
        assertThat(output.getInsertedId(), nullValue());

        // the write itself went through
        try (MongoClient client = getMongoClient()) {
            assertThat(
                client.getDatabase(database).getCollection("unack_insert").countDocuments(Filters.eq("name", "John Doe")),
                is(1L)
            );
        }
    }

    @Test
    void updateUnacknowledged() throws Exception {
        RunContext runContext = runContextFactory.of();
        String database = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        try (MongoClient client = getMongoClient()) {
            client.getDatabase(database).getCollection("unack_update")
                .insertOne(new Document("name", "John Doe").append("visits", 1));
        }

        Update update = Update.builder()
            .connection(unacknowledgedConnection())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("unack_update"))
            .document("{\"$inc\": {\"visits\": 1}}")
            .filter(ImmutableMap.of("name", "John Doe"))
            .build();

        Update.Output output = update.run(runContext);

        assertThat(output.getWasAcknowledged(), is(false));
        assertThat(output.getUpsertedId(), nullValue());
        assertThat(output.getMatchedCount(), is(0L));
        assertThat(output.getModifiedCount(), nullValue());

        // the $inc was applied
        try (MongoClient client = getMongoClient()) {
            Document doc = client.getDatabase(database).getCollection("unack_update")
                .find(Filters.eq("name", "John Doe")).first();
            assertThat(doc.getInteger("visits"), is(2));
        }
    }

    @Test
    void deleteUnacknowledged() throws Exception {
        RunContext runContext = runContextFactory.of();
        String database = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        try (MongoClient client = getMongoClient()) {
            client.getDatabase(database).getCollection("unack_delete")
                .insertOne(new Document("name", "John Doe"));
        }

        Delete delete = Delete.builder()
            .connection(unacknowledgedConnection())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("unack_delete"))
            .filter(ImmutableMap.of("name", "John Doe"))
            .build();

        Delete.Output output = delete.run(runContext);

        assertThat(output.getWasAcknowledged(), is(false));
        assertThat(output.getDeletedCount(), is(0L));

        // the delete went through
        try (MongoClient client = getMongoClient()) {
            assertThat(
                client.getDatabase(database).getCollection("unack_delete").countDocuments(),
                is(0L)
            );
        }
    }

    @Test
    void loadUnacknowledged() throws Exception {
        RunContext runContext = runContextFactory.of();
        String database = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        // 5 records with chunk 2: before the fix only the first chunk was written and the task failed
        File tempFile = File.createTempFile("unacknowledged_load_", ".trs");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            for (int i = 0; i < 5; i++) {
                FileSerde.write(output, ImmutableMap.of("name", "john-" + i));
            }
        }
        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Load load = Load.builder()
            .connection(unacknowledgedConnection())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("unack_load"))
            .from(Property.ofValue(uri.toString()))
            .chunk(Property.ofValue(2))
            .build();

        Load.Output output = load.run(runContext);

        assertThat(output.getSize(), is(5L));
        assertThat(output.getInsertedCount(), is(0));

        // the whole file was written, not just the first chunk
        try (MongoClient client = getMongoClient()) {
            assertThat(
                client.getDatabase(database).getCollection("unack_load").countDocuments(),
                is(5L)
            );
        }
    }

    @Test
    void bulkUnacknowledged() throws Exception {
        RunContext runContext = runContextFactory.of();
        String database = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        File tempFile = File.createTempFile("unacknowledged_bulk_", ".trs");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            for (int i = 0; i < 5; i++) {
                output.write(("{ insertOne: { \"document\": { \"name\": \"john-" + i + "\" } } }\n").getBytes(StandardCharsets.UTF_8));
            }
        }
        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Bulk bulk = Bulk.builder()
            .connection(unacknowledgedConnection())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("unack_bulk"))
            .from(Property.ofValue(uri.toString()))
            .chunk(Property.ofValue(2))
            .build();

        Bulk.Output output = bulk.run(runContext);

        assertThat(output.getSize(), is(5L));
        assertThat(output.getInsertedCount(), is(0));

        try (MongoClient client = getMongoClient()) {
            assertThat(
                client.getDatabase(database).getCollection("unack_bulk").countDocuments(),
                is(5L)
            );
        }
    }
}
