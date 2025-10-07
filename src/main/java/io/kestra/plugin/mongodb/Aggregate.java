package io.kestra.plugin.mongodb;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import java.time.Duration;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;

import jakarta.validation.constraints.NotNull;
import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run aggregation pipeline on a MongoDB collection."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Simple aggregation pipeline to group and sum data",
            code = """
                id: mongodb_aggregate
                namespace: company.team
                
                tasks:
                  - id: aggregate
                    type: io.kestra.plugin.mongodb.Aggregate
                    connection:
                      uri: "mongodb://root:example@localhost:27017/?authSource=admin"
                    database: "my_database"
                    collection: "sales"
                    pipeline:
                      - $match:
                          status: "active"
                      - $group:
                          _id: "$category"
                          total: 
                            $sum: "$amount"
                          count:
                            $sum: 1
                      - $sort:
                          total: -1
                """
        ),
        @Example(
            full = true,
            title = "Complex aggregation with lookup and data transformation",
            code = """
                id: mongodb_complex_aggregate
                namespace: company.team
                
                tasks:
                  - id: aggregate_with_lookup
                    type: io.kestra.plugin.mongodb.Aggregate
                    connection:
                      uri: "mongodb://root:example@localhost:27017/?authSource=admin"
                    database: "my_database"
                    collection: "users"
                    pipeline:
                      - $lookup:
                          from: "orders"
                          localField: "_id"
                          foreignField: "userId"
                          as: "userOrders"
                      - $addFields:
                          totalOrders:
                            $size: "$userOrders"
                          totalSpent:
                            $sum: "$userOrders.amount"
                      - $project:
                          name: 1
                          email: 1
                          totalOrders: 1
                          totalSpent: 1
                      - $match:
                          totalOrders:
                            $gt: 0
                    allowDiskUse: true
                    maxTimeMs: 30000
                """
        )
    }
)
public class Aggregate extends AbstractTask implements RunnableTask<Aggregate.Output> {
    @Schema(
        title = "MongoDB aggregation pipeline",
        description = "List of pipeline stages as a BSON array or list of maps"
    )
    @NotNull
    Property<List<Map<String, Object>>> pipeline;

    @Schema(
        title = "Whether to allow disk usage for stages",
        description = "Enables writing to temporary files when a pipeline stage exceeds the 100 megabyte limit"
    )
    @Builder.Default
    private Property<Boolean> allowDiskUse = Property.ofValue(true);

    @Schema(
        title = "Maximum execution time in milliseconds",
        description = "Sets the maximum execution time on the server for this operation"
    )
    @Builder.Default
    private Property<Integer> maxTimeMs = Property.ofValue((int) Duration.ofSeconds(60).toMillis());

    @Schema(
        title = "Batch size for cursor",
        description = "Sets the number of documents to return per batch"
    )
    @Builder.Default
    private Property<Integer> batchSize = Property.ofValue(1000);

    @Schema(
        title = "Whether to store the data from the aggregation result into an Ion-serialized data file"
    )
    @Builder.Default
    private Property<FetchType> store = Property.ofValue(FetchType.FETCH);

    @Override
    public Aggregate.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        try (MongoClient client = this.connection.client(runContext)) {
            MongoCollection<BsonDocument> collection = this.collection(runContext, client, BsonDocument.class);

            List<Bson> pipelineStages = new ArrayList<>();
            if (this.pipeline != null) {
                List<Map<String, Object>> renderedPipeline = runContext.render(this.pipeline).asList(Map.class);
                for (Map<String, Object> stage : renderedPipeline) {
                    BsonDocument bsonStage = MongoDbService.toDocument(runContext, stage);
                    pipelineStages.add(bsonStage);
                    logger.debug("Pipeline stage: {}", bsonStage);
                }
            }

            AggregateIterable<BsonDocument> aggregate = collection.aggregate(pipelineStages);

            Boolean allowDisk = runContext.render(this.allowDiskUse).as(Boolean.class).orElse(true);
            if (allowDisk) {
                aggregate.allowDiskUse(true);
            }

            Integer maxTime = runContext.render(this.maxTimeMs).as(Integer.class).orElse(60000);
            if (maxTime > 0) {
                aggregate.maxTime(maxTime, java.util.concurrent.TimeUnit.MILLISECONDS);
            }

            Integer batch = runContext.render(this.batchSize).as(Integer.class).orElse(1000);
            if (batch > 0) {
                aggregate.batchSize(batch);
            }

            Output.OutputBuilder outputBuilder = Output.builder();

            switch (runContext.render(this.store).as(FetchType.class).orElse(FetchType.FETCH)) {
                case FETCH:
                    Pair<List<Object>, Long> fetch = this.fetch(aggregate);
                    outputBuilder
                        .rows(fetch.getLeft())
                        .size(fetch.getRight());
                    break;

                case STORE:
                    Pair<URI, Long> store = this.store(runContext, aggregate);
                    outputBuilder
                        .uri(store.getLeft())
                        .size(store.getRight());
                    break;

                default:
                    // FETCH_ONE and NONE not implemented for aggregation
                    Pair<List<Object>, Long> defaultFetch = this.fetch(aggregate);
                    outputBuilder
                        .rows(defaultFetch.getLeft())
                        .size(defaultFetch.getRight());
                    break;
            }

            Output output = outputBuilder.build();

            runContext.metric(Counter.of(
                "records", output.getSize(),
                "database", collection.getNamespace().getDatabaseName(),
                "collection", collection.getNamespace().getCollectionName()
            ));

            return output;
        }
    }

    private Pair<URI, Long> store(RunContext runContext, AggregateIterable<BsonDocument> documents) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (var output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {
            var flux = Flux.fromIterable(documents).map(document -> MongoDbService.map(document.toBsonDocument()));
            Long count = FileSerde.writeAll(output, flux).block();

            return Pair.of(
                runContext.storage().putFile(tempFile),
                count
            );
        }
    }

    private Pair<List<Object>, Long> fetch(AggregateIterable<BsonDocument> documents) {
        List<Object> result = new ArrayList<>();
        AtomicLong count = new AtomicLong();

        documents
            .forEach(throwConsumer(bsonDocument -> {
                count.incrementAndGet();
                result.add(MongoDbService.map(bsonDocument.toBsonDocument()));
            }));

        return Pair.of(
            result,
            count.get()
        );
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "List containing the aggregation results",
            description = "Only populated if `store` parameter is set to false"
        )
        private List<Object> rows;

        @Schema(
            title = "The number of documents returned by the aggregation"
        )
        private Long size;

        @Schema(
            title = "URI of the file containing the aggregation results",
            description = "Only populated if `store` parameter is set to true"
        )
        private URI uri;
    }
}