package io.kestra.plugin.mongodb;

import java.time.Duration;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonInclude;
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
    description = """
        Periodically runs a MongoDB find; if results are non-empty, starts a Flow with the rows or stored file. Uses Find task behavior (filter/projection/sort/limit/skip). Default interval is 60s and store is false, returning rows in trigger output.

        By default, every poll that returns documents starts an execution, including for documents an earlier poll already delivered. Set `watermarkField` to deliver each document once."""
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
        ),
        @Example(
            title = "Deliver each new or updated order once, using its `updatedAt` field as the watermark.",
            full = true,
            code = """
                id: mongodb_trigger_watermark
                namespace: company.team

                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.rows }}"
                    tasks:
                      - id: log
                        type: io.kestra.plugin.core.log.Log
                        message: "{{ taskrun.value }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.mongodb.Trigger
                    interval: "PT1M"
                    connection:
                      uri: "{{ secret('MONGODB_URI') }}"
                    database: shop
                    collection: orders
                    filter:
                      status: paid
                    watermarkField: updatedAt
                    limit: 500
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
        description = """
            BSON string or map rendered before execution.
            A null map entry is dropped when the flow is parsed; pass a JSON string when a null is needed, for example `'{"deletedAt": null}'`.
            """
    )
    @PluginProperty(group = "processing")
    @JsonInclude(value = JsonInclude.Include.NON_NULL, content = JsonInclude.Include.ALWAYS)
    private Object filter;

    @Schema(
        title = "Projection",
        description = """
            BSON string or map selecting fields to return.
            A null map entry is dropped when the flow is parsed; pass a JSON string when a null is needed.
            """
    )
    @PluginProperty(group = "advanced")
    @JsonInclude(value = JsonInclude.Include.NON_NULL, content = JsonInclude.Include.ALWAYS)
    private Object projection;

    @Schema(
        title = "Sort",
        description = """
            BSON string or map defining sort order.
            A null map entry is dropped when the flow is parsed; pass a JSON string when a null is needed.
            """
    )
    @PluginProperty(group = "advanced")
    @JsonInclude(value = JsonInclude.Include.NON_NULL, content = JsonInclude.Include.ALWAYS)
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

    @Schema(
        title = "Watermark field",
        description = """
            Field used to deliver each document only once, such as `updatedAt`, a sequence number or `_id`. A dotted path reaches a nested field.
            When set, the trigger stores the position of the last delivered document in the namespace KV Store and only returns the documents after it, read in the order of this field and then `_id`; `limit` caps how many each poll returns. It can't be combined with `sort` or `skip`.
            Use a field whose value only increases when a document is added or updated, with a single type across documents: a document written later with a lower value is never returned, nor is one whose value has another type than the stored position (a string among dates, for example). ObjectId `_id` values are only ordered to the second across clients. Documents where the field is missing or null are never returned.
            On a large collection, create an index on the fields `filter` matches by equality, then this field, then `_id` (for example `{status: 1, updatedAt: 1, _id: 1}`), so that each poll doesn't scan and sort the whole collection; `_id` alone needs no extra index.
            When not set, every poll that returns documents starts an execution, including for documents an earlier poll already delivered."""
    )
    @PluginProperty(group = "processing")
    private Property<String> watermarkField;

    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private transient volatile MongoClient client;

    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private transient volatile MongoCursor<BsonDocument> cursor;

    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private transient volatile MongoDbSessionKiller sessionKiller;

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

        var watermark = this.watermark(runContext, context);

        MongoDbSessionKiller mongoSessionKiller = new MongoDbSessionKiller(this.connection.clientSettings(runContext));
        MongoClient mongoClient = this.connection.client(runContext, mongoSessionKiller);
        this.sessionKiller = mongoSessionKiller;
        this.client = mongoClient;
        try (mongoClient) {
            Find.Output output = find.run(runContext, mongoClient, mongoCursor -> this.cursor = mongoCursor, watermark != null ? watermark : Find.QueryHook.NONE);

            logger.debug("Found '{}' rows", output.getSize());

            if (Optional.ofNullable(output.getSize()).orElse(0L) == 0) {
                return Optional.empty();
            }

            var execution = TriggerService.generateExecution(this, conditionContext, context, output);

            // saved once the execution exists, so a poll that can't start one doesn't skip its documents
            if (watermark != null) {
                watermark.save();
            }

            return Optional.of(execution);
        } finally {
            this.cursor = null;
            this.client = null;
            this.sessionKiller = null;
        }
    }

    // null when watermarkField isn't set
    private TriggerWatermark watermark(RunContext runContext, TriggerContext context) throws Exception {
        var rWatermarkField = runContext.render(this.watermarkField).as(String.class).filter(field -> !field.isBlank());
        if (rWatermarkField.isEmpty()) {
            return null;
        }

        if (this.sort != null) {
            throw new IllegalArgumentException("`sort` can't be combined with `watermarkField`: the trigger reads documents in watermark order so that none is skipped.");
        }
        if (runContext.render(this.skip).as(Integer.class).orElse(0) > 0) {
            throw new IllegalArgumentException("`skip` can't be combined with `watermarkField`: skipped documents would never be delivered.");
        }

        return TriggerWatermark.load(runContext, context.getNamespace(), context.getFlowId(), this.id, rWatermarkField.get());
    }

    /**
     * Best-effort cancellation of an in-flight evaluation, without blocking the calling worker thread.
     *
     * <p>
     * Closing the cursor and the client alone does not interrupt an in-flight find() or getMore(): the server
     * keeps running it and the read only returns once it is done. After closing them, the client's server
     * sessions are therefore killed, which makes the server interrupt that operation and close the cursor, so
     * the evaluation fails promptly. A genuinely wedged server or network read is still not interrupted. See
     * {@link MongoDbSessionKiller} for when killing the sessions is not possible.
     *
     * <p>
     * This work runs on a dedicated daemon thread because closing and killing are synchronous round trips
     * while this plugin configures no client-side timeout bound. This method therefore always returns
     * promptly and never throws.
     *
     * <p>
     * Each kill() invocation spawns one daemon thread, but only the first one per evaluation sends
     * {@code killSessions}: a genuinely wedged server with no socket timeout may hold that daemon thread
     * indefinitely. Because the thread is a daemon, it never keeps JVM shutdown alive.
     */
    @Override
    public void kill() {
        MongoCursor<BsonDocument> mongoCursor = this.cursor;
        MongoClient mongoClient = this.client;
        MongoDbSessionKiller mongoSessionKiller = this.sessionKiller;
        if (mongoCursor == null && mongoClient == null) {
            return;
        }

        Thread killThread = new Thread(
            () ->
            {
                closeQuietly(mongoCursor, mongoClient);
                if (mongoSessionKiller != null) {
                    mongoSessionKiller.killSessions();
                }
            },
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
