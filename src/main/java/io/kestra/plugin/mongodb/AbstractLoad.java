package io.kestra.plugin.mongodb;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.conversions.Bson;
import org.slf4j.Logger;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.WriteModel;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.RunnableTaskException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
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
    @PluginProperty(internalStorageURI = true, group = "main")
    private Property<String> from;

    @Schema(
        title = "Bulk chunk size",
        description = "Number of write models sent per bulkWrite call; defaults to 1000."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Integer> chunk = Property.ofValue(1000);

    abstract protected Flux<WriteModel<Bson>> source(RunContext runContext, InputStream inputStream) throws Exception;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        try (
            MongoClient client = this.connection.client(runContext);
            InputStream inputStream = new BufferedInputStream(runContext.storage().getFile(from), FileSerde.BUFFER_SIZE)
        ) {
            MongoCollection<Bson> collection = this.collection(runContext, client);

            AtomicLong count = new AtomicLong();
            Progress progress = new Progress();

            var renderedChunk = runContext.render(this.chunk).as(Integer.class).orElse(null);
            Flux<BulkWriteResult> flowable = this.source(runContext, inputStream)
                .doOnNext(docWriteRequest ->
                {
                    count.incrementAndGet();
                })
                .buffer(renderedChunk, renderedChunk)
                .map(indexRequests ->
                {
                    List<WriteModel<Bson>> bulkOperations = new ArrayList<>(indexRequests);

                    return progress.write(collection, bulkOperations);
                });

            try {
                flowable.blockLast();
            } catch (WriteFailure e) {
                // the failed task keeps the outputs, so they show what was applied before the failure
                metrics(runContext, collection, progress.requests, progress.applied);
                throw new RunnableTaskException(e.getMessage(), e.getCause(), progress.output(progress.applied));
            } catch (RuntimeException e) {
                metrics(runContext, collection, progress.requests, progress.applied);
                throw e;
            }

            // metrics & finalize
            metrics(runContext, collection, progress.requests, count.get());

            logger.info(
                "Successfully sent {} requests for {} records",
                progress.requests,
                count.get()
            );

            return progress.output(count.get());
        }
    }

    private static void metrics(RunContext runContext, MongoCollection<Bson> collection, long requests, long records) {
        runContext.metric(
            Counter.of(
                "requests.count", requests,
                "database", collection.getNamespace().getDatabaseName(),
                "collection", collection.getNamespace().getCollectionName()
            )
        );
        runContext.metric(
            Counter.of(
                "records", records,
                "database", collection.getNamespace().getDatabaseName(),
                "collection", collection.getNamespace().getCollectionName()
            )
        );
    }

    // records are read ahead of the chunk being written, so only applied tells how far the run got
    private static class Progress {
        private long requests;
        private long applied;
        private int insertedCount;
        private int matchedCount;
        private int modifiedCount;
        private int deletedCount;
        private int upsertedCount;

        BulkWriteResult write(MongoCollection<Bson> collection, List<WriteModel<Bson>> chunk) {
            var records = range(this.applied + 1, this.applied + chunk.size());
            this.requests++;

            BulkWriteResult result;
            try {
                result = collection.bulkWrite(chunk);
            } catch (MongoBulkWriteException e) {
                this.add(e.getWriteResult());

                if (e.getWriteErrors().isEmpty()) {
                    this.applied += chunk.size();
                    var writeConcernError = e.getWriteConcernError() != null ? e.getWriteConcernError().getMessage() : e.getMessage();
                    throw new WriteFailure(
                        "MongoDB reported a write concern error for the chunk of " + records + ": its records were applied but may not be durable (" +
                            count(this.applied) + " applied in total): " + writeConcernError,
                        e
                    );
                }

                // the bulkWrite is ordered, so the operations before the failing one are applied and none after it
                var error = e.getWriteErrors().get(0);
                this.applied += error.getIndex();
                throw new WriteFailure("Record " + (this.applied + 1) + " failed (" + count(this.applied) + " applied before it): " + error.getMessage(), e);
            } catch (RuntimeException e) {
                // no write result, for example after a network error or a failover
                throw new WriteFailure(
                    "The chunk of " + records + " failed and MongoDB returned no result, so it's unknown which of its records were applied (" +
                        count(this.applied) + " applied before it): " + e.getMessage(),
                    e
                );
            }

            this.add(result);
            this.applied += chunk.size();
            return result;
        }

        private void add(BulkWriteResult result) {
            // unacknowledged writes (w=0) carry no ids or counts: every getter on the
            // result throws, so skip accumulation and let the remaining chunks flow through
            if (!result.wasAcknowledged()) {
                return;
            }

            this.insertedCount += result.getInsertedCount();
            this.matchedCount += result.getMatchedCount();
            this.modifiedCount += result.getModifiedCount();
            this.deletedCount += result.getDeletedCount();
            this.upsertedCount += result.getUpserts().size();
        }

        Output output(long size) {
            return Output.builder()
                .size(size)
                .matchedCount(this.matchedCount)
                .insertedCount(this.insertedCount)
                .modifiedCount(this.modifiedCount)
                .deletedCount(this.deletedCount)
                .upsertedCount(this.upsertedCount)
                .build();
        }

        // record numbers count from 1 across the whole source file
        private static String range(long first, long last) {
            return first == last ? "record " + first : "records " + first + " to " + last;
        }

        private static String count(long records) {
            return records == 1 ? "1 record" : records + " records";
        }
    }

    private static class WriteFailure extends RuntimeException {
        WriteFailure(String message, Throwable cause) {
            super(message, cause);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Rows processed",
            description = "Total records read from the source file. When a write fails, the records applied before it."
        )
        private Long size;

        @Schema(
            title = "Inserted documents",
            description = "Count of documents created by bulk writes. 0 for unacknowledged writes (`w=0`), which have no counts to report."
        )
        @Builder.Default
        private int insertedCount = 0;

        @Schema(
            title = "Matched documents",
            description = "Documents matched by update or replacement models. 0 for unacknowledged writes (`w=0`), which have no counts to report."
        )
        @Builder.Default
        private int matchedCount = 0;

        @Schema(
            title = "Deleted documents",
            description = "Documents removed by bulk delete models. 0 for unacknowledged writes (`w=0`), which have no counts to report."
        )
        @Builder.Default
        private int deletedCount = 0;

        @Schema(
            title = "Modified documents",
            description = "Documents updated by bulk operations. 0 for unacknowledged writes (`w=0`), which have no counts to report."
        )
        @Builder.Default
        private int modifiedCount = 0;

        @Schema(
            title = "Upserted documents",
            description = "Documents created by update or replacement models with `upsert: true` that matched no document. Only `Bulk` sends these models, so it's always 0 for `Load`. 0 for unacknowledged writes (`w=0`), which have no counts to report."
        )
        @Builder.Default
        private int upsertedCount = 0;
    }
}
