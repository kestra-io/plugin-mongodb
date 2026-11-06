package io.kestra.plugin.mongodb;

import java.lang.reflect.Proxy;
import java.util.List;

import org.bson.BsonMaximumSizeExceededException;
import org.bson.Document;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteInsert;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DriverRejectionTest {
    @Test
    void oversizedDocumentInSingleChunkReportsOffendingRecord() {
        var collection = mockCollection(chunk ->
        {
            throw new BsonMaximumSizeExceededException("Document size of 17825807 is larger than maximum of 16777216.");
        });

        var progress = new AbstractLoad.Progress(true);
        var chunk = List.<WriteModel<Bson>> of(
            new InsertOneModel<>(new Document("big", new byte[17 * 1024 * 1024]))
        );

        var ex = assertThrows(AbstractLoad.WriteFailure.class, () -> progress.write(collection, chunk));

        assertThat(
            ex.getMessage(),
            is("Record 1 was rejected by the driver (0 records applied before it): Document size of 17825807 is larger than maximum of 16777216.")
        );
        var output = progress.output(0);
        assertThat(output.getSize(), is(0L));
        assertThat(output.getInsertedCount(), is(0));
    }

    @Test
    void oversizedDocumentInMultiRecordChunkPinpointsFailingRecord() {
        var collection = mockCollection(chunk ->
        {
            throw new BsonMaximumSizeExceededException("Document size of 17825807 is larger than maximum of 16777216.");
        });

        // Simulate that 2 records were already applied in a previous chunk
        var progress = new AbstractLoad.Progress(true);
        var firstChunk = List.<WriteModel<Bson>> of(
            new InsertOneModel<>(new Document("a", 1)),
            new InsertOneModel<>(new Document("b", 2))
        );
        var successMock = new BulkWriteResult() {
            @Override
            public boolean wasAcknowledged() {
                return true;
            }

            @Override
            public int getInsertedCount() {
                return 2;
            }

            @Override
            public int getMatchedCount() {
                return 0;
            }

            @Override
            public int getDeletedCount() {
                return 0;
            }

            @Override
            public int getModifiedCount() {
                return 0;
            }

            @Override
            public List<BulkWriteInsert> getInserts() {
                return List.of();
            }

            @Override
            public List<BulkWriteUpsert> getUpserts() {
                return List.of();
            }
        };
        var successCollection = (MongoCollection<Bson>) Proxy.newProxyInstance(
            DriverRejectionTest.class.getClassLoader(),
            new Class<?>[] { MongoCollection.class },
            (p, m, args) -> switch (m.getName()) {
                case "bulkWrite" -> successMock;
                case "getCodecRegistry" -> MongoClientSettings.getDefaultCodecRegistry();
                case "getWriteConcern" -> WriteConcern.ACKNOWLEDGED;
                default -> null;
            }
        );
        progress.write(successCollection, firstChunk);

        // Second chunk: records 3, 4, 5, where record 4 (index 1) is oversized
        var secondChunk = List.<WriteModel<Bson>> of(
            new InsertOneModel<>(new Document("c", 3)),
            new InsertOneModel<>(new Document("big", new byte[17 * 1024 * 1024])),
            new InsertOneModel<>(new Document("e", 5))
        );

        var ex = assertThrows(AbstractLoad.WriteFailure.class, () -> progress.write(collection, secondChunk));

        assertThat(
            ex.getMessage(),
            is("Record 4 was rejected by the driver (3 records applied before it): Document size of 17825807 is larger than maximum of 16777216.")
        );
        var output = progress.output(3);
        assertThat(output.getSize(), is(3L));
        assertThat(output.getInsertedCount(), is(3));
    }

    @Test
    void unencodableTypeInMultiRecordChunkPinpointsFailingRecord() {
        var collection = mockCollection(chunk ->
        {
            throw new CodecConfigurationException("Can not find a codec for class java.lang.Object.");
        });

        var progress = new AbstractLoad.Progress(true);
        var chunk = List.<WriteModel<Bson>> of(
            new InsertOneModel<>(new Document("valid", 1)),
            new InsertOneModel<>(new Document("invalid", new Object())),
            new InsertOneModel<>(new Document("valid2", 3))
        );

        var ex = assertThrows(AbstractLoad.WriteFailure.class, () -> progress.write(collection, chunk));

        assertThat(
            ex.getMessage(),
            is("Record 2 was rejected by the driver (it's unknown whether an earlier batch was applied): Can not find a codec for class java.lang.Object.")
        );
    }

    @Test
    void invalidUpdateWithoutOperatorInMultiRecordChunkPinpointsFailingRecord() {
        var collection = mockCollection(chunk ->
        {
            throw new IllegalArgumentException("Invalid BSON field name name: All update operators must start with '$', but 'name' does not");
        });

        var progress = new AbstractLoad.Progress(true);
        var chunk = List.<WriteModel<Bson>> of(
            new UpdateOneModel<>(new Document("_id", 1), new Document("$set", new Document("name", "valid"))),
            new UpdateOneModel<>(new Document("_id", 2), new Document("name", "invalid")),
            new UpdateOneModel<>(new Document("_id", 3), new Document("$set", new Document("name", "valid2")))
        );

        var ex = assertThrows(AbstractLoad.WriteFailure.class, () -> progress.write(collection, chunk));

        assertThat(
            ex.getMessage(),
            is(
                "Record 2 was rejected by the driver (it's unknown whether an earlier batch was applied): Invalid BSON field name name: All update operators must start with '$', but 'name' does not"
            )
        );
    }

    @Test
    void singleRecordChunkDriverRejectionReportsAppliedCount() {
        var collection = mockCollection(chunk ->
        {
            throw new CodecConfigurationException("Can not find a codec for class java.lang.Object.");
        });

        var progress = new AbstractLoad.Progress(true);
        var chunk = List.<WriteModel<Bson>> of(
            new InsertOneModel<>(new Document("invalid", new Object()))
        );

        var ex = assertThrows(AbstractLoad.WriteFailure.class, () -> progress.write(collection, chunk));

        assertThat(
            ex.getMessage(),
            is("Record 1 was rejected by the driver (0 records applied before it): Can not find a codec for class java.lang.Object.")
        );
    }

    @Test
    void clientDriverRejectionWithoutIsolatedRecordNamesChunk() {
        var collection = mockCollection(chunk ->
        {
            throw new IllegalArgumentException("Driver rejected operation due to client-side rule.");
        });

        var progress = new AbstractLoad.Progress(true);
        var chunk = List.<WriteModel<Bson>> of(
            new InsertOneModel<>(new Document("a", 1)),
            new InsertOneModel<>(new Document("b", 2)),
            new InsertOneModel<>(new Document("c", 3))
        );

        var ex = assertThrows(AbstractLoad.WriteFailure.class, () -> progress.write(collection, chunk));

        assertThat(
            ex.getMessage(),
            is("The chunk of records 1 to 3 was rejected by the driver (it's unknown whether an earlier batch was applied): Driver rejected operation due to client-side rule.")
        );
    }

    @Test
    void networkFailureStillReportsNoResultAndUnknownApplied() {
        var collection = mockCollection(chunk ->
        {
            throw new MongoSocketReadException("connection reset", new ServerAddress());
        });

        var progress = new AbstractLoad.Progress(true);
        var chunk = List.<WriteModel<Bson>> of(
            new InsertOneModel<>(new Document("a", 1)),
            new InsertOneModel<>(new Document("b", 2))
        );

        var ex = assertThrows(AbstractLoad.WriteFailure.class, () -> progress.write(collection, chunk));

        assertThat(
            ex.getMessage(),
            is("The chunk of records 1 to 2 failed and MongoDB returned no result, so it's unknown which of its records were applied (0 records applied before it): connection reset")
        );
    }

    @Test
    void mongoTimeoutExceptionReportsUnknownAppliedInsteadOfDriverRejection() {
        var collection = mockCollection(chunk ->
        {
            throw new MongoTimeoutException("Timed out after 30000 ms while waiting for a server that matches ReadPreferenceServerSelector.");
        });

        var progress = new AbstractLoad.Progress(true);
        var chunk = List.<WriteModel<Bson>> of(
            new InsertOneModel<>(new Document("a", 1)),
            new InsertOneModel<>(new Document("b", 2))
        );

        var ex = assertThrows(AbstractLoad.WriteFailure.class, () -> progress.write(collection, chunk));

        assertThat(
            ex.getMessage(),
            is(
                "The chunk of records 1 to 2 failed and MongoDB returned no result, so it's unknown which of its records were applied (0 records applied before it): Timed out after 30000 ms while waiting for a server that matches ReadPreferenceServerSelector."
            )
        );
    }

    @FunctionalInterface
    private interface ThrowingConsumer<T> {
        void accept(T t) throws Exception;
    }

    @SuppressWarnings("unchecked")
    private static MongoCollection<Bson> mockCollection(ThrowingConsumer<List<WriteModel<Bson>>> onBulkWrite) {
        return (MongoCollection<Bson>) Proxy.newProxyInstance(
            DriverRejectionTest.class.getClassLoader(),
            new Class<?>[] { MongoCollection.class },
            (proxy, method, args) ->
            {
                if (method.getName().equals("bulkWrite")) {
                    onBulkWrite.accept((List<WriteModel<Bson>>) args[0]);
                    return null;
                }
                if (method.getName().equals("getCodecRegistry")) {
                    return MongoClientSettings.getDefaultCodecRegistry();
                }
                if (method.getName().equals("getWriteConcern")) {
                    return WriteConcern.ACKNOWLEDGED;
                }
                if (method.getName().equals("toString")) {
                    return "MockMongoCollection";
                }
                if (method.getName().equals("hashCode")) {
                    return System.identityHashCode(proxy);
                }
                if (method.getName().equals("equals")) {
                    return proxy == args[0];
                }
                throw new UnsupportedOperationException(method.getName());
            }
        );
    }
}
