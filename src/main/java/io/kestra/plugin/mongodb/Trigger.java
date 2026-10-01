package io.kestra.plugin.mongodb;

import java.time.Duration;
import java.util.Optional;

import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

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
    private static final Logger log = LoggerFactory.getLogger(Trigger.class);

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

        Find find = Find.builder()
            .id(this.id)
            .type(Find.class.getName())
            .connection(this.connection)
            .database(this.database)
            .collection(this.collection)
            .filter(this.filter)
            .projection(this.projection)
            .sort(this.sort)
            .limit(this.limit)
            .skip(this.skip)
            .store(this.store)
            .build();

        MongoClient mongoClient = this.connection.client(runContext);
        this.client = mongoClient;
        try (mongoClient) {
            Find.Output output = find.run(runContext, mongoClient, mongoCursor -> this.cursor = mongoCursor);

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

    /**
     * Best-effort cancellation of an in-flight evaluation, without blocking the calling worker thread.
     *
     * <p>
     * Closing the cursor requests driver teardown of the cursor and closing the client releases its
     * resources, but neither is guaranteed to interrupt a genuinely wedged server or network read: an
     * already in-flight getMore() may continue until it returns, with teardown completing afterwards,
     * and a connection currently checked out and blocked may not be reclaimed immediately.
     *
     * <p>
     * The closes run on a dedicated daemon thread because closing may itself perform a synchronous
     * teardown round trip while this plugin configures no client-side timeout bound. This method therefore
     * always returns promptly and never throws.
     */
    @Override
    public void kill() {
        MongoCursor<BsonDocument> mongoCursor = this.cursor;
        MongoClient mongoClient = this.client;
        if (mongoCursor == null && mongoClient == null) {
            return;
        }

        Thread killThread = new Thread(
            () -> closeQuietly(mongoCursor, mongoClient),
            "kestra-mongodb-trigger-kill"
        );
        killThread.setDaemon(true);
        killThread.start();
    }

    private static void closeQuietly(MongoCursor<BsonDocument> mongoCursor, MongoClient mongoClient) {
        if (mongoCursor != null) {
            try {
                mongoCursor.close();
            } catch (Exception e) {
                log.debug("Failed to close MongoDB cursor during kill()", e);
                // closing a cursor is idempotent, never fail kill()
            }
        }

        if (mongoClient != null) {
            try {
                mongoClient.close();
            } catch (Exception e) {
                log.debug("Failed to close MongoDB client during kill()", e);
                // closing a client is idempotent, never fail kill()
            }
        }
    }

}
