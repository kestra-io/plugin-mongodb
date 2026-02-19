package io.kestra.plugin.mongodb;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.*;
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
    title = "Query documents from MongoDB",
    description = "Executes find on a collection with optional projection, sort, skip, and limit. Filter/projection/sort accept BSON strings or maps rendered from Flow variables. Results are fetched in-memory by default or stored as Ion in internal storage when `store` is true."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: mongodb_find
                namespace: company.team

                tasks:
                  - id: find
                    type: io.kestra.plugin.mongodb.Find
                    connection:
                      uri: "mongodb://root:example@localhost:27017/?authSource=admin"
                    database: "my_database"
                    collection: "my_collection"
                    filter:
                      _id:
                        $oid: 60930c39a982931c20ef6cd6
                """
        ),
        @Example(
            full = true,
            title = "Find documents in MongoDB based on a filter condition using [MongoDB Query Language](https://www.mongodb.com/docs/manual/tutorial/query-documents/).",
            code = """
                id: filter_mongodb
                namespace: company.team

                tasks:
                  - id: filter
                    type: io.kestra.plugin.mongodb.Find
                    connection:
                      uri: mongodb://host.docker.internal:27017/
                    database: local
                    collection: pokemon
                    store: true
                    filter:
                      base_experience:
                        $gt: 100
                """
        )
    },
    metrics = {
        @Metric(
            name = "records",
            type = Counter.TYPE,
            unit = "count",
            description = "Number of documents fetched from MongoDB"
        )
    }
)
public class Find extends AbstractTask implements RunnableTask<Find.Output> {
    @Schema(
        title = "Query filter",
        description = "BSON string or map rendered before execution."
    )
    @PluginProperty(dynamic = true)
    private Object filter;

    @Schema(
        title = "Projection",
        description = "BSON string or map selecting fields to return."
    )
    @PluginProperty(dynamic = true)
    private Object projection;

    @Schema(
        title = "Sort",
        description = "BSON string or map defining sort order."
    )
    @PluginProperty(dynamic = true)
    private Object sort;

    @Schema(
        title = "Limit",
        description = "Maximum documents returned."
    )
    private Property<Integer> limit;

    @Schema(
        title = "Skip",
        description = "Documents to skip before returning results."
    )
    private Property<Integer> skip;


    @Schema(
        title = "Store results",
        description = "When true, writes results as Ion to internal storage; otherwise returns rows. Defaults to false."
    )
    @Builder.Default
    private Property<Boolean> store = Property.ofValue(false);

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

            if (runContext.render(this.limit).as(Integer.class).isPresent()) {
                find.limit(runContext.render(this.limit).as(Integer.class).get());
            }

            if (runContext.render(this.skip).as(Integer.class).isPresent()) {
                find.skip(runContext.render(this.skip).as(Integer.class).get());
            }

            Output.OutputBuilder builder = Output.builder();

            if (runContext.render(this.store).as(Boolean.class).orElseThrow()) {
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

        try (var output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {
            var flux = Flux.fromIterable(documents).map(document -> MongoDbService.map(document.toBsonDocument()));
            Long count = FileSerde.writeAll(output, flux).block();

            return Pair.of(
                runContext.storage().putFile(tempFile),
                count
            );
        }
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
            title = "Result rows",
            description = "Present when store is false."
        )
        private List<Object> rows;

        @Schema(
            title = "Rows fetched"
        )
        private Long size;

        @Schema(
            title = "Stored result URI",
            description = "Internal storage URI when store is true."
        )
        private URI uri;
    }
}
