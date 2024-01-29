package io.kestra.plugin.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.micronaut.core.annotation.Introspected;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;

import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete one or many documents"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "connection:",
                "  uri: \"mongodb://root:example@localhost:27017/?authSource=admin\"",
                "database: \"my_database\"",
                "collection: \"my_collection\"",
                "operation: \"DELETE_ONE\"",
                "filter:",
                "  _id:",
                "    $oid: 60930c39a982931c20ef6cd6",
            }
        ),
    }
)
public class Delete extends AbstractTask implements RunnableTask<Delete.Output> {
    @Schema(
        title = "The mongodb bson filter",
        description = "Can be a bson string, or a map"
    )
    @PluginProperty(dynamic = true)
    private Object filter;

    @Schema(
        title = "Operation to use"
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    @NotNull
    private Operation operation = Operation.DELETE_ONE;

    @Override
    public Delete.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        try (MongoClient client = this.connection.client(runContext)) {
            MongoCollection<Bson> collection = this.collection(runContext, client);

            BsonDocument bsonFilter = MongoDbService.toDocument(runContext, this.filter);

            DeleteResult deleteResult;
            if (this.operation == Operation.DELETE_ONE) {
                deleteResult = collection.deleteOne(bsonFilter);
            } else {
                deleteResult = collection.deleteMany(bsonFilter);
            }

            logger.debug("Delete doc with filter: {}", bsonFilter);

            runContext.metric(Counter.of(
                "deleted.count", deleteResult.getDeletedCount(),
                "database", collection.getNamespace().getDatabaseName(),
                "collection", collection.getNamespace().getCollectionName()
            ));

            return Output.builder()
                .wasAcknowledged(deleteResult.wasAcknowledged())
                .deletedCount(deleteResult.getDeletedCount())
                .build();
        }
    }

    @Introspected
    public enum Operation {
        DELETE_ONE,
        DELETE_MANY
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "true if the write was acknowledged."
        )
        private Boolean wasAcknowledged;

        @Schema(
            title = "The number of documents deleted."
        )
        private final long deletedCount;
    }
}
