package io.kestra.plugin.mongodb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Locale;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;
import com.mongodb.client.MongoClient;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

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
}
