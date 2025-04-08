package io.kestra.plugin.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.UpdateResult;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;

import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Update or Replace one or many documents in a MongoDB collection."
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
                    document: "{"$set": { "tags": ["blue", "green", "red"]}}"
                """
        ),
    }
)
public class Update extends AbstractTask implements RunnableTask<Update.Output> {
    @Schema(
        title = "MongoDB document.",
        description = "Can be a BSON string or a map."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Object document;

    @Schema(
        title = "MongoDB BSON filter.",
        description = "Can be a BSON string or a map."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Object filter;

    @Schema(
        title = "Operation to use."
    )
    @Builder.Default
    private Property<Operation> operation = Property.of(Operation.UPDATE_ONE);

    @Override
    public Update.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        try (MongoClient client = this.connection.client(runContext)) {
            MongoCollection<Bson> collection = this.collection(runContext, client);

            BsonDocument bsonDocument = MongoDbService.toDocument(runContext, this.document);
            BsonDocument bsonFilter = MongoDbService.toDocument(runContext, this.filter);

            UpdateResult updateResult;
            if (Operation.REPLACE_ONE.equals(runContext.render(operation).as(Operation.class).orElseThrow())) {
                updateResult = collection.replaceOne(bsonFilter, bsonDocument);
            } else if (Operation.UPDATE_ONE.equals(runContext.render(operation).as(Operation.class).orElseThrow())) {
                updateResult = collection.updateOne(bsonFilter, bsonDocument);
            } else {
                updateResult = collection.updateMany(bsonFilter, bsonDocument);
            }

            logger.debug("Updating doc: {} with filter: {}", bsonDocument, bsonFilter);

            runContext.metric(Counter.of(
                "updated.count", updateResult.getModifiedCount(),
                "database", collection.getNamespace().getDatabaseName(),
                "collection", collection.getNamespace().getCollectionName()
            ));

            return Output.builder()
                .upsertedId(updateResult.getUpsertedId() != null ? updateResult.getUpsertedId().asObjectId().getValue().toString() : null)
                .wasAcknowledged(updateResult.wasAcknowledged())
                .matchedCount(updateResult.getMatchedCount())
                .modifiedCount(updateResult.getModifiedCount())
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
            title = "The upserted ID.",
            description = "Will be null for `replace` operation."
        )
        @Nullable
        private String upsertedId;

        @Schema(
            title = "Whether the write was acknowledged."
        )
        private Boolean wasAcknowledged;

        @Schema(
            title = "The number of documents matched by the query."
        )
        private final long matchedCount;

        @Schema(
            title = "The number of documents modified by the update."
        )
        private final Long modifiedCount;
    }
}
