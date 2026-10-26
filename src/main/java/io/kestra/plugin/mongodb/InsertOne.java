package io.kestra.plugin.mongodb;


import com.fasterxml.jackson.annotation.JsonInclude;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;

import com.mongodb.MongoWriteConcernException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.InsertOneResult;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.RunnableTaskException;
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
                    document: "{{ outputs.task_id.data | toJson }}"
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
        description = """
            BSON string or map rendered before execution.
            A null map entry is dropped when the flow is parsed; pass a JSON string when a null is needed, for example `'{"_id": 1, "k": null}'`.
            """
    )
    @PluginProperty(dynamic = true, group = "main")
    @NotNull
    @JsonInclude(value = JsonInclude.Include.NON_NULL, content = JsonInclude.Include.ALWAYS)
    private Object document;

    @Override
    public InsertOne.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        try (MongoClient client = this.client(runContext)) {
            MongoCollection<Bson> collection = this.collection(runContext, client);

            BsonDocument bsonDocument = MongoDbService.toDocument(runContext, this.document);
            InsertOneResult insertOneResult;
            try {
                insertOneResult = collection.insertOne(MongoDbService.withId(bsonDocument));
            } catch (MongoWriteConcernException e) {
                // the document was written on the primary but the requested write concern was not met in time;
                // keep the output (the client-side _id) so the task does not fail empty and a retry does not duplicate it (see AbstractLoad)
                throw new RunnableTaskException(
                    "The insert was applied on the primary but the requested write concern was not met in time, so it may not be durable: " + e.getMessage(),
                    e,
                    Output.builder()
                        .insertedId(MongoDbService.toIdString(bsonDocument.get("_id")))
                        .wasAcknowledged(true)
                        .build()
                );
            }

            logger.info("Insert doc: {}", bsonDocument);

            runContext.metric(
                Counter.of(
                    "inserted.count", 1,
                    "database", collection.getNamespace().getDatabaseName(),
                    "collection", collection.getNamespace().getCollectionName()
                )
            );

            // withId set the _id client-side, so it is known even when a w=0 result cannot report it
            return Output.builder()
                .insertedId(MongoDbService.toIdString(bsonDocument.get("_id")))
                .wasAcknowledged(insertOneResult.wasAcknowledged())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Inserted document id",
            description = """
                The `_id` of the inserted document, as a string:
                - ObjectId: its hex string
                - String: as is
                - Int32, Int64, Double or Decimal128: the number
                - UUID (Binary subtype 4): its canonical string
                - any other type, including a legacy UUID (Binary subtype 3) or a document: its relaxed MongoDB Extended JSON, such as `{"a": 1}`

                With an unacknowledged write concern (`w=0`) it is the id that was sent, the insert itself is not confirmed."""
        )
        private String insertedId;

        @Schema(
            title = "Whether the write was acknowledged"
        )
        private Boolean wasAcknowledged;
    }
}
