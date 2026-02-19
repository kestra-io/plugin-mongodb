package io.kestra.plugin.mongodb;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.WriteModel;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.bson.conversions.Bson;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import jakarta.validation.constraints.NotNull;
import reactor.core.publisher.Flux;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractLoad extends AbstractTask implements RunnableTask<AbstractLoad.Output> {
    @Schema(
        title = "Source file",
        description = "Path in internal storage (kestra://...) containing BSON or JSON documents to stream into MongoDB."
    )
    @NotNull
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @Schema(
        title = "Bulk chunk size",
        description = "Number of write models sent per bulkWrite call; defaults to 1000."
    )
    @Builder.Default
    private Property<Integer> chunk = Property.ofValue(1000);

    abstract protected Flux<WriteModel<Bson>> source(RunContext runContext, BufferedReader inputStream) throws Exception;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        try (
            MongoClient client = this.connection.client(runContext);
            BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from)), FileSerde.BUFFER_SIZE)
        ) {
            MongoCollection<Bson> collection = this.collection(runContext, client);

            AtomicLong count = new AtomicLong();
            AtomicInteger matchedCount = new AtomicInteger();
            AtomicInteger insertedCount = new AtomicInteger();
            AtomicInteger modifiedCount = new AtomicInteger();
            AtomicInteger deletedCount = new AtomicInteger();

            var renderedChunk = runContext.render(this.chunk).as(Integer.class).orElse(null);
            Flux<BulkWriteResult> flowable = this.source(runContext, inputStream)
                .doOnNext(docWriteRequest -> {
                    count.incrementAndGet();
                })
                .buffer(renderedChunk, renderedChunk)
                .map(indexRequests -> {
                    List<WriteModel<Bson>> bulkOperations = new ArrayList<>(indexRequests);

                    return collection.bulkWrite(bulkOperations);
                })
                .doOnNext(bulkItemResponse -> {
                    matchedCount.addAndGet(bulkItemResponse.getMatchedCount());
                    insertedCount.addAndGet(bulkItemResponse.getInsertedCount());
                    modifiedCount.addAndGet(bulkItemResponse.getModifiedCount());
                    deletedCount.addAndGet(bulkItemResponse.getDeletedCount());
                });

            // metrics & finalize
            Long requestCount = flowable.count().block();
            runContext.metric(Counter.of(
                "requests.count", requestCount,
                "database", collection.getNamespace().getDatabaseName(),
                "collection", collection.getNamespace().getCollectionName()
            ));
            runContext.metric(Counter.of(
                "records", count.get(),
                "database", collection.getNamespace().getDatabaseName(),
                "collection", collection.getNamespace().getCollectionName()
            ));

            logger.info(
                "Successfully sent {} requests for {} records",
                requestCount,
                count.get()
            );

            return Output.builder()
                .size(count.get())
                .matchedCount(matchedCount.get())
                .insertedCount(insertedCount.get())
                .modifiedCount(modifiedCount.get())
                .deletedCount(deletedCount.get())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Rows processed",
            description = "Total records read from the source file."
        )
        private Long size;

        @Schema(
            title = "Inserted documents",
            description = "Count of documents created by bulk writes."
        )
        @Builder.Default
        private int insertedCount = 0;

        @Schema(
            title = "Matched documents",
            description = "Documents matched by update or replacement models."
        )
        @Builder.Default
        private int matchedCount = 0;

        @Schema(
            title = "Deleted documents",
            description = "Documents removed by bulk delete models."
        )
        @Builder.Default
        private int deletedCount = 0;

        @Schema(
            title = "Modified documents",
            description = "Documents updated by bulk operations."
        )
        @Builder.Default
        private int modifiedCount = 0;
    }
}
