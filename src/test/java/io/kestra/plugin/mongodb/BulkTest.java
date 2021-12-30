package io.kestra.plugin.mongodb;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class BulkTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of();
        String database = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        try (OutputStream output = new FileOutputStream(tempFile)) {
            output.write(("{ insertOne: { \"document\": { \"_id\" : 1, \"char\" : \"Brisbane\", \"class\" : \"monk\", \"lvl\" : 4 } } }\n").getBytes(StandardCharsets.UTF_8));
            output.write(("{ insertOne: { \"document\": { \"_id\" : 2, \"char\" : \"Eldon\", \"class\" : \"alchemist\", \"lvl\" : 3 } } }\n").getBytes(StandardCharsets.UTF_8));
            output.write(("{ insertOne: { \"document\": { \"_id\" : 3, \"char\" : \"Meldane\", \"class\" : \"ranger\", \"lvl\" : 3 } } }\n").getBytes(StandardCharsets.UTF_8));
            output.write(("{ insertOne: { \"document\": { \"_id\": 4, \"char\": \"Dithras\", \"class\": \"barbarian\", \"lvl\": 4 } } }\n").getBytes(StandardCharsets.UTF_8));
            output.write(("{ insertOne: { \"document\": { \"_id\": 5, \"char\": \"Taeln\", \"class\": \"fighter\", \"lvl\": 3 } } }\n").getBytes(StandardCharsets.UTF_8));
            output.write(("{ updateOne : {\"filter\" : { \"char\" : \"Eldon\" },\"update\" : { $set : { \"status\" : \"Critical Injury\" } }} }\n").getBytes(StandardCharsets.UTF_8));
            output.write(("{ deleteOne : { \"filter\" : { \"char\" : \"Brisbane\"} } }\n").getBytes(StandardCharsets.UTF_8));
            output.write(("{ replaceOne : {\"filter\" : { \"char\" : \"Meldane\" },\"replacement\" : { \"char\" : \"Tanys\", \"class\" : \"oracle\", \"lvl\": 4 }} }\n").getBytes(StandardCharsets.UTF_8));
        }

        URI uri = storageInterface.put(URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Bulk put = Bulk.builder()
            .connection(MongoDbConnection.builder()
                .uri("mongodb://root:example@localhost:27017/?authSource=admin")
                .build())
            .database(database)
            .collection("bulk")
            .from(uri.toString())
            .chunk(10)
            .build();

        Bulk.Output runOutput = put.run(runContext);

        assertThat(runOutput.getSize(), is(8L));
        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("requests.count")).findFirst().orElseThrow().getValue(), is(1D));
        assertThat(runContext.metrics().stream().filter(e -> e.getName().equals("records")).findFirst().orElseThrow().getValue(), is(8D));
    }
}
