package io.kestra.plugin.mongodb;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.Pair;
import org.bson.BsonDocument;
import org.slf4j.Logger;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Poll MongoDB and trigger on results",
    description = "Periodically runs a MongoDB find; if results are non-empty, starts a Flow with the rows or stored file. Uses Find task behavior (filter/projection/sort/limit/skip). Default interval is 60s and store is false, returning rows in trigger output."
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for a MongoDB query to return results, and then iterate through returned documents.",
            full = true,
            code = """
                id: mongodb_trigger
                namespace: company.team

                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.rows }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ fromJson(taskrun.value) }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.mongodb.Trigger
                    interval: "PT5M"
                    connection:
                      uri: mongodb://root:example@localhost:27017/?authSource=admin
                    database: samples
                    collection: books
                    filter:
                      pageCount:
                        $gte: 50
                    sort:
                      pageCount: -1
                    projection:
                      title: 1
                      publishedDate: 1
                      pageCount: 1
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Find.Output> {

    @Schema(
        title = "Polling interval",
        description = "Duration between queries; defaults to PT60S."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(
        title = "MongoDB connection"
    )
    @PluginProperty(group = "connection")
    private MongoDbConnection connection;

    @Schema(
        title = "Database name"
    )
    @PluginProperty(group = "connection")
    private Property<String> database;

    @Schema(
        title = "Collection name"
    )
    @PluginProperty(group = "advanced")
    private Property<String> collection;

    @Schema(
        title = "Query filter",
        description = "BSON string or map rendered before execution."
    )
    @PluginProperty(group = "processing")
    private Object filter;

    @Schema(
        title = "Projection",
        description = "BSON string or map selecting fields to return."
    )
    @PluginProperty(group = "advanced")
    private Object projection;

    @Schema(
        title = "Sort",
        description = "BSON string or map defining sort order."
    )
    @PluginProperty(group = "advanced")
    private Object sort;

    @Schema(
        title = "Limit",
        description = "Maximum documents returned."
    )
    @PluginProperty(group = "advanced")
    private Property<Integer> limit;

    @Schema(
        title = "Skip",
        description = "Documents to skip before returning results."
    )
    @PluginProperty(group = "advanced")
    private Property<Integer> skip;

    @Schema(
        title = "Store results",
        description = "When true, writes results as Ion to internal storage; otherwise rows are kept in output. Defaults to false."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> store = Property.ofValue(false);

    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private transient volatile MongoClient client;

    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private transient volatile MongoCursor<BsonDocument> cursor;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        MongoClient mongoClient = this.connection.client(runContext);
        this.client = mongoClient;
        try (mongoClient) {
            MongoDatabase database = mongoClient.getDatabase(runContext.render(this.database).as(String.class).orElseThrow());
            MongoCollection<BsonDocument> collection = database.getCollection(
                runContext.render(this.collection).as(String.class).orElseThrow(),
                BsonDocument.class
            );

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

            Find.Output.OutputBuilder builder = Find.Output.builder();

            if (runContext.render(this.store).as(Boolean.class).orElseThrow()) {
                Pair<URI, Long> stored = this.store(runContext, find);

                builder
                    .uri(stored.getLeft())
                    .size(stored.getRight());
            } else {
                Pair<ArrayList<Object>, Long> fetched = this.fetch(find);

                builder
                    .rows(fetched.getLeft())
                    .size(fetched.getRight());
            }

            Find.Output output = builder.build();

            runContext.metric(
                Counter.of(
                    "records", output.getSize(),
                    "database", collection.getNamespace().getDatabaseName(),
                    "collection", collection.getNamespace().getCollectionName()
                )
            );

            logger.debug("Found '{}' rows", output.getSize());

            if (Optional.ofNullable(output.getSize()).orElse(0L) == 0) {
                return Optional.empty();
            }

            return Optional.of(
                TriggerService.generateExecution(this, conditionContext, context, output)
            );
        } finally {
            this.cursor = null;
            this.client = null;
        }
    }

    @Override
    public void kill() {
        MongoCursor<BsonDocument> mongoCursor = this.cursor;
        if (mongoCursor != null) {
            try {
                mongoCursor.close();
            } catch (Exception ignored) {
                // cursor close is idempotent, never fail kill()
            }
        }

        MongoClient mongoClient = this.client;
        if (mongoClient != null) {
            try {
                mongoClient.close();
            } catch (Exception ignored) {
                // client close is idempotent, never fail kill()
            }
        }
    }

    private Pair<URI, Long> store(RunContext runContext, FindIterable<BsonDocument> documents) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (var output = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)) {
            MongoCursor<BsonDocument> mongoCursor = documents.cursor();
            this.cursor = mongoCursor;
            try {
                var flux = Flux.fromIterable((Iterable<BsonDocument>) () -> mongoCursor)
                    .map(document -> MongoDbService.map(document.toBsonDocument()));
                Long count = FileSerde.writeAll(output, flux).block();

                return Pair.of(
                    runContext.storage().putFile(tempFile),
                    count
                );
            } finally {
                try {
                    mongoCursor.close();
                } catch (Exception ignored) {
                    // kill() may have closed the cursor concurrently, close is idempotent
                }
                if (this.cursor == mongoCursor) {
                    this.cursor = null;
                }
            }
        }
    }

    private Pair<ArrayList<Object>, Long> fetch(FindIterable<BsonDocument> documents) {
        ArrayList<Object> result = new ArrayList<>();
        AtomicLong count = new AtomicLong();

        MongoCursor<BsonDocument> mongoCursor = documents.cursor();
        this.cursor = mongoCursor;
        try {
            while (mongoCursor.hasNext()) {
                BsonDocument bsonDocument = mongoCursor.next();
                count.incrementAndGet();
                result.add(MongoDbService.map(bsonDocument.toBsonDocument()));
            }
        } finally {
            try {
                mongoCursor.close();
            } catch (Exception ignored) {
                // kill() may have closed the cursor concurrently, close is idempotent
            }
            if (this.cursor == mongoCursor) {
                this.cursor = null;
            }
        }

        return Pair.of(
            result,
            count.get()
        );
    }

}
