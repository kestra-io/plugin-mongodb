package io.kestra.plugin.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.InsertOneResult;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;

import java.util.Objects;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Insert one document into MongoDB",
    description = "Renders a BSON document from Flow variables and inserts it with insertOne. Requires write access; document may be provided as JSON/BSON string or map."
)
@Plugin(
    examples = {
        @Example(
            title = "Insert a document with a map.",
            full = true,
            code = """
                id: mongodb_insertone
                namespace: company.team
                
                tasks:
                  - id: insertone
                    type: io.kestra.plugin.mongodb.InsertOne
                    connection:
                      uri: "mongodb://root:example@localhost:27017/?authSource=admin"
                    database: "my_database"
                    collection: "my_collection"
                    document:
                      _id:
                        $oid: 60930c39a982931c20ef6cd6
                      name: "John Doe"
                      city: "Paris"   
                """
        ),
        @Example(
            title = "Insert a document from a JSON string.",
            full = true,
            code = """
                id: mongodb_insertone
                namespace: company.team
                
                tasks:
                  - id: insertone
                    type: io.kestra.plugin.mongodb.InsertOne
                    connection:
                      uri: "mongodb://root:example@localhost:27017/?authSource=admin"
                    database: "my_database"
                    collection: "my_collection"
                    document: "{{ outputs.task_id.data | json }}"
                """
        ),
    },
    metrics = {
        @Metric(
            name = "inserted.count",
            type = Counter.TYPE,
            unit = "count",
            description = "Number of documents inserted into MongoDB"
        )
    }
)
public class InsertOne extends AbstractTask implements RunnableTask<InsertOne.Output> {
    @Schema(
        title = "Document to insert",
        description = "BSON string or map rendered before execution."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Object document;

    @Override
    public InsertOne.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        try (MongoClient client = this.connection.client(runContext)) {
            MongoCollection<Bson> collection = this.collection(runContext, client);

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
            title = "Inserted document id"
        )
        private String insertedId;

        @Schema(
            title = "Whether the write was acknowledged"
        )
        private Boolean wasAcknowledged;
    }
}
