package io.kestra.plugin.mongodb;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Locale;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class LoadTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of();
        String database = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        for (int i = 0; i < 100; i++) {
            FileSerde.write(output, ImmutableMap.of(
                "id", new ObjectId().toString(),
                "name", "john"
            ));
        }
        URI uri = storageInterface.put(URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Load put = Load.builder()
            .connection(MongoDbConnection.builder()
                .uri("mongodb://root:example@localhost:27017/?authSource=admin")
                .build())
            .database(database)
            .collection("load")
            .from(uri.toString())
            .chunk(10)
            .build();

        Load.Output runOutput = put.run(runContext);

        assertThat(runOutput.getSize(), is(100L));
        assertThat(runOutput.getInsertedCount(), is(100));
        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("requests.count")).findFirst().orElseThrow().getValue(), is(10D));
        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("records")).findFirst().orElseThrow().getValue(), is(100D));
    }
}
