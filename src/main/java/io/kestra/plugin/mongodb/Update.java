package io.kestra.plugin.mongodb;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.UpdateResult;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Update or replace MongoDB documents",
    description = "Runs MongoDB replaceOne, updateOne, or updateMany on a collection. Defaults to UPDATE_ONE; both filter and document accept BSON maps or strings and are rendered from Flow variables. Requires write access; upserts are not exposed."
)
@Plugin(
    examples = {
        @Example(
            title = "Replace a document.",
            full = true,
            code = """
                id: mongodb_update
                namespace: company.team

                tasks:
                  - id: update
                    type: io.kestra.plugin.mongodb.Update
                    connection:
                      uri: "mongodb://root:example@localhost:27017/?authSource=admin"
                    database: "my_database"
                    collection: "my_collection"
                    operation: "REPLACE_ONE"
                    document:
                      _id:
                        $oid: 60930c39a982931c20ef6cd6
                      name: "John Doe"
                      city: "Paris"
                    filter:
                      _id:
                        $oid: 60930c39a982931c20ef6cd6
                """
        ),
        @Example(
            title = "Update a document.",
            full = true,
            code = """
                id: mongodb_update
                namespace: company.team

                tasks:
                  - id: update
                    type: io.kestra.plugin.mongodb.Update
                    connection:
                      uri: "mongodb://root:example@localhost:27017/?authSource=admin"
                    database: "my_database"
                    collection: "my_collection"
                    filter:
                      _id:
                        $oid: 60930c39a982931c20ef6cd6
                    document: '{"$set": { "tags": ["blue", "green", "red"]}}'
                """
        ),
    },
    metrics = {
        @Metric(
            name = "updated.count",
            type = Counter.TYPE,
            unit = "count",
            description = "Number of documents updated in MongoDB"
        )
    }
)
public class Update extends AbstractTask implements RunnableTask<Update.Output> {
    @Schema(
        title = "Update payload or replacement document",
        description = """
            BSON string or map rendered before execution.
            An empty `$set` fails instead of reporting success on a no-op.
            A null map entry is dropped when the flow is parsed; pass a JSON string when a null is needed, for example `'{"$set": {"k": null}}'`.
            """
    )
    @PluginProperty(dynamic = true, group = "main")
    @NotNull
    @JsonInclude(value = JsonInclude.Include.NON_NULL, content = JsonInclude.Include.ALWAYS)
    private Object document;

    @Schema(
        title = "Query filter",
        description = """
            BSON string or map rendered before execution; selects documents to update.
            A null map entry is dropped when the flow is parsed; pass a JSON string when a null is needed, for example `'{"deletedAt": null}'`.
            """
    )
    @PluginProperty(dynamic = true, group = "main")
    @NotNull
    @JsonInclude(value = JsonInclude.Include.NON_NULL, content = JsonInclude.Include.ALWAYS)
    private Object filter;

    @Schema(
        title = "Update operation",
        description = "One of UPDATE_ONE (default), UPDATE_MANY, or REPLACE_ONE."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Operation> operation = Property.ofValue(Operation.UPDATE_ONE);

    @Override
    public Update.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        var rOperation = runContext.render(this.operation).as(Operation.class).orElseThrow();

        BsonDocument bsonDocument = MongoDbService.toDocument(runContext, this.document);
        BsonDocument bsonFilter = MongoDbService.toDocument(runContext, this.filter);
        // An empty $set can come from a null dropped in core, so fail instead of a silent no-op.
        if (bsonDocument.containsKey("$set") && bsonDocument.get("$set").isDocument() && bsonDocument.getDocument("$set").isEmpty()) {
            throw new IllegalArgumentException("`$set` must not be empty: it changes nothing and can come from a dropped null. Pass `document` as a JSON string when a null is needed, for example '{\"$set\": {\"k\": null}}'.");
        }

        try (MongoClient client = this.connection.client(runContext)) {
            MongoCollection<Bson> collection = this.collection(runContext, client);

            UpdateResult updateResult;
            if (Operation.REPLACE_ONE.equals(rOperation)) {
                updateResult = collection.replaceOne(bsonFilter, bsonDocument);
            } else if (Operation.UPDATE_ONE.equals(rOperation)) {
                updateResult = collection.updateOne(bsonFilter, bsonDocument);
            } else {
                updateResult = collection.updateMany(bsonFilter, bsonDocument);
            }

            logger.debug("Updating doc: {} with filter: {}", bsonDocument, bsonFilter);

            // an unacknowledged write (w=0) carries no counts: every getter on it throws,
            // including getModifiedCount() used for the metric below
            boolean acknowledged = updateResult.wasAcknowledged();

            if (acknowledged) {
                runContext.metric(
                    Counter.of(
                        "updated.count", updateResult.getModifiedCount(),
                        "database", collection.getNamespace().getDatabaseName(),
                        "collection", collection.getNamespace().getCollectionName()
                    )
                );
            }

            return Output.builder()
                .upsertedId(acknowledged && updateResult.getUpsertedId() != null ? MongoDbService.toIdString(updateResult.getUpsertedId()) : null)
                .wasAcknowledged(acknowledged)
                .matchedCount(acknowledged ? updateResult.getMatchedCount() : 0L)
                .modifiedCount(acknowledged ? updateResult.getModifiedCount() : null)
                .build();
        }
    }

    public enum Operation {
        REPLACE_ONE,
        UPDATE_ONE,
        UPDATE_MANY
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Upserted document id",
            description = """
                Null unless MongoDB created a document during the write. Otherwise the `_id` of that document, as a string:
                - ObjectId: its hex string
                - String: as is
                - Int32, Int64, Double or Decimal128: the number
                - UUID (Binary subtype 4): its canonical string
                - any other type, including a legacy UUID (Binary subtype 3) or a document: its relaxed MongoDB Extended JSON, such as `{"a": 1}`

                Always null for unacknowledged writes (`w=0`), which have no id to report."""
        )
        @Nullable
        private String upsertedId;

        @Schema(
            title = "Whether the write was acknowledged"
        )
        private Boolean wasAcknowledged;

        @Schema(
            title = "The number of documents matched by the query",
            description = "0 for unacknowledged writes (`w=0`), which have no counts to report."
        )
        private final long matchedCount;

        @Schema(
            title = "The number of documents modified by the update",
            description = "Null for unacknowledged writes (`w=0`), which have no counts to report."
        )
        private final Long modifiedCount;
    }
}
