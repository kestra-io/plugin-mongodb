package io.kestra.plugin.mongodb;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Find documents from a MongoDB collection."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "connection:",
                "  uri: \"mongodb://root:example@localhost:27017/?authSource=admin\"",
                "database: \"my_database\"",
                "collection: \"my_collection\"",
                "filter:",
                "  _id:",
                "    $oid: 60930c39a982931c20ef6cd6",
            }
        ),
    }
)
public class Find extends AbstractTask implements RunnableTask<Find.Output> {
    @Schema(
        title = "MongoDB BSON filter.",
        description = "Can be a BSON string, or a map."
    )
    @PluginProperty(dynamic = true)
    private Object filter;

    @Schema(
        title = "MongoDB BSON projection.",
        description = "Can be a BSON string, or a map."
    )
    @PluginProperty(dynamic = true)
    private Object projection;

    @Schema(
        title = "MongoDB BSON sort.",
        description = "Can be a BSON string, or a map."
    )
    @PluginProperty(dynamic = true)
    private Object sort;

    @Schema(
        title = "The number of records to return."
    )
    @PluginProperty(dynamic = true)
    private Integer limit;

    @Schema(
        title = "The number of records to skip."
    )
    @PluginProperty(dynamic = true)
    private Integer skip;


    @Schema(
        title = "Whether to store the data from the query result into an ion serialized data file."
    )
    @PluginProperty
    @Builder.Default
    private Boolean store = false;

    @Override
    public Find.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        try (MongoClient client = this.connection.client(runContext)) {
            MongoCollection<BsonDocument> collection = this.collection(runContext, client, BsonDocument.class);

            BsonDocument bsonFilter = MongoDbService.toDocument(runContext, this.filter);
            logger.debug("Find: {}", bsonFilter);

            FindIterable<BsonDocument> find = collection.find(bsonFilter);

            if (this.projection != null) {
                find.projection(MongoDbService.toDocument(runContext, this.projection));
            }

            if (this.sort != null) {
                find.sort(MongoDbService.toDocument(runContext, this.sort));
            }

            if (this.limit != null) {
                find.limit(this.limit);
            }

            if (this.skip != null) {
                find.skip(this.skip);
            }

            Output.OutputBuilder builder = Output.builder();

            if (this.store) {
                Pair<URI, Long> store = this.store(runContext, find);

                builder
                    .uri(store.getLeft())
                    .size(store.getRight());
            } else {
                Pair<ArrayList<Object>, Long> fetch = this.fetch(find);

                builder
                    .rows(fetch.getLeft())
                    .size(fetch.getRight());
            }

            Output output = builder
                .build();

            runContext.metric(Counter.of(
                "records", output.getSize(),
                "database", collection.getNamespace().getDatabaseName(),
                "collection", collection.getNamespace().getCollectionName()
            ));

            return output;
        }
    }

    private Pair<URI, Long> store(RunContext runContext, FindIterable<BsonDocument> documents) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        AtomicLong count = new AtomicLong();

        try (OutputStream output = new FileOutputStream(tempFile)) {
            documents.forEach(throwConsumer(bsonDocument -> {
                count.incrementAndGet();
                FileSerde.write(output, MongoDbService.map(bsonDocument.toBsonDocument()));
            }));
        }

        return Pair.of(
            runContext.storage().putFile(tempFile),
            count.get()
        );
    }

    private Pair<ArrayList<Object>, Long> fetch(FindIterable<BsonDocument> documents) {
        ArrayList<Object> result = new ArrayList<>();
        AtomicLong count = new AtomicLong();

        documents
            .forEach(throwConsumer(bsonDocument -> {
                count.incrementAndGet();
                result.add(MongoDbService.map(bsonDocument.toBsonDocument()));
            }));

        return Pair.of(
            result,
            count.get()
        );
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "List containing the fetched data.",
            description = "Only populated if `store` parameter is set to false."
        )
        private List<Object> rows;

        @Schema(
            title = "The number of rows fetched."
        )
        private Long size;

        @Schema(
            title = "URI of the file containing the fetched results.",
            description = "Only populated if `store` parameter is set to true."
        )
        private URI uri;
    }
}
