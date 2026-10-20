package io.kestra.plugin.mongodb;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete documents from MongoDB",
    description = "Runs deleteOne (default) or deleteMany on a collection. Filter supports BSON strings or maps rendered from Flow variables."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: mongodb_delete
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.mongodb.Delete
                    connection:
                      uri: "mongodb://root:example@localhost:27017/?authSource=admin"
                    database: "my_database"
                    collection: "my_collection"
                    operation: "DELETE_ONE"
                    filter:
                      _id:
                        $oid: 60930c39a982931c20ef6cd6
                """
        ),
    },
    metrics = {
        @Metric(
            name = "deleted.count",
            type = Counter.TYPE,
            unit = "count",
            description = "Number of documents deleted from the MongoDB collection"
        )
    }
)
public class Delete extends AbstractTask implements RunnableTask<Delete.Output> {
    @Schema(
        title = "Query filter",
        description = """
            BSON string or map rendered before execution.
            Required with DELETE_MANY: an empty map filter fails, set `filter: '{}'` (string) to delete every document in the collection.
            A null map entry is dropped when the flow is parsed; pass a JSON string when a null is needed, for example `'{"deletedAt": null}'`.
            """
    )
    @PluginProperty(dynamic = true, group = "processing")
    @JsonInclude(value = JsonInclude.Include.NON_NULL, content = JsonInclude.Include.ALWAYS)
    private Object filter;

    @Schema(
        title = "Delete operation",
        description = "DELETE_ONE (default) or DELETE_MANY."
    )
    @Builder.Default
    @NotNull
    @PluginProperty(group = "advanced")
    private Property<Operation> operation = Property.ofValue(Operation.DELETE_ONE);

    @Override
    public Delete.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        var rOperation = runContext.render(this.operation).as(Operation.class).orElseThrow();
        if (this.filter == null && Operation.DELETE_MANY.equals(rOperation)) {
            throw new IllegalArgumentException("`filter` is required with DELETE_MANY: set `filter: '{}'` to delete every document in the collection.");
        }

        BsonDocument bsonFilter = MongoDbService.toDocument(runContext, this.filter);
        // An empty map can come from a null dropped in core, so only the string form may delete everything.
        if (Operation.DELETE_MANY.equals(rOperation) && this.filter instanceof Map && bsonFilter.isEmpty()) {
            throw new IllegalArgumentException("`filter` must not be an empty map with DELETE_MANY: it deletes every document and can come from a dropped null. Set `filter: '{}'` to delete every document in the collection.");
        }

        try (MongoClient client = this.connection.client(runContext)) {
            MongoCollection<Bson> collection = this.collection(runContext, client);

            DeleteResult deleteResult;
            if (Operation.DELETE_ONE.equals(rOperation)) {
                deleteResult = collection.deleteOne(bsonFilter);
            } else {
                deleteResult = collection.deleteMany(bsonFilter);
            }

            logger.debug("Delete doc with filter: {}", bsonFilter);

            runContext.metric(
                Counter.of(
                    "deleted.count", deleteResult.getDeletedCount(),
                    "database", collection.getNamespace().getDatabaseName(),
                    "collection", collection.getNamespace().getCollectionName()
                )
            );

            return Output.builder()
                .wasAcknowledged(deleteResult.wasAcknowledged())
                .deletedCount(deleteResult.getDeletedCount())
                .build();
        }
    }

    public enum Operation {
        DELETE_ONE,
        DELETE_MANY
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Whether the write was acknowledged"
        )
        private Boolean wasAcknowledged;

        @Schema(
            title = "Documents deleted"
        )
        private final long deletedCount;
    }
}
