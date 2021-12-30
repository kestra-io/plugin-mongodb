package io.kestra.plugin.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.InsertOneResult;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.bson.BsonDocument;
import org.slf4j.Logger;

import java.util.Objects;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Insert one document"
)
@Plugin(
    examples = {
        @Example(
            title = "Insert a document with a map",
            code = {
                "connection:",
                "  uri: \"mongodb://root:example@localhost:27017/?authSource=admin\"",
                "database: \"my_database\"",
                "collection: \"my_collection\"",
                "document:",
                "  _id:",
                "    $oid: 60930c39a982931c20ef6cd6",
                "  name: \"John Doe\"",
                "  city: \"Paris\"",
            }
        ),
        @Example(
            title = "Insert a document from a json string",
            code = {
                "connection:",
                "  uri: \"mongodb://root:example@localhost:27017/?authSource=admin\"",
                "database: \"my_database\"",
                "collection: \"my_collection\"",
                "document: \"{{ outputs.task_id.data | json }}\""
            }
        ),
    }
)
public class InsertOne extends AbstractTask implements RunnableTask<InsertOne.Output> {
    @Schema(
        title = "The mongodb document",
        description = "Can be a bson string, or a map"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Object document;

    @Override
    public InsertOne.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        try (MongoClient client = this.connection.client(runContext)) {
            MongoCollection<BsonDocument> collection = this.collection(runContext, client);

            BsonDocument bsonDocument = MongoDbService.toDocument(runContext, this.document);
            InsertOneResult insertOneResult = collection.insertOne(bsonDocument);

            logger.info("Insert doc: {}", bsonDocument);

            runContext.metric(Counter.of(
                "inserted.count", 1,
                "database", collection.getNamespace().getDatabaseName(),
                "collection", collection.getNamespace().getCollectionName()
            ));

            return Output.builder()
                .insertedId(Objects.requireNonNull(insertOneResult.getInsertedId()).asObjectId().getValue().toString())
                .wasAcknowledged(insertOneResult.wasAcknowledged())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The inserted Id"
        )
        private String insertedId;

        @Schema(
            title = "true if the write was acknowledged."
        )
        private Boolean wasAcknowledged;
    }
}
