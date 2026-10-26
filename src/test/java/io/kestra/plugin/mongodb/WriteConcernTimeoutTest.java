package io.kestra.plugin.mongodb;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import com.mongodb.MongoWriteConcernException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcernResult;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTaskException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import lombok.experimental.SuperBuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

// On a write-concern timeout the write committed on the primary but the requested write concern was not met in time.
// InsertOne, Update and Delete must then fail but keep their outputs (not empty) so the flow can see what was applied,
// mirroring AbstractLoad. The client is proxied to throw MongoWriteConcernException so no replica set is needed.
@KestraTest
class WriteConcernTimeoutTest extends MongoDbContainer {
    private static String database() {
        return "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);
    }

    @Test
    void insertOneKeepsTheIdOnAWriteConcernTimeout() throws Exception {
        var insert = InsertOne.builder()
            .connection(WriteConcernTimeoutConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(database()))
            .collection(Property.ofValue("wc"))
            .document(Map.of("_id", "doc1", "name", "jane"))
            .build();

        var e = assertThrows(RunnableTaskException.class, () -> insert.run(runContextFactory.of()));

        assertThat(e.getMessage(), containsString("may not be durable"));
        var output = (InsertOne.Output) e.getOutput();
        assertThat(output, notNullValue());
        assertThat(output.getInsertedId(), is("doc1"));
        assertThat(output.getWasAcknowledged(), is(true));
    }

    @Test
    void updateKeepsItsCountsOnAWriteConcernTimeout() throws Exception {
        var update = Update.builder()
            .connection(WriteConcernTimeoutConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(database()))
            .collection(Property.ofValue("wc"))
            .operation(Property.ofValue(Update.Operation.UPDATE_MANY))
            .filter(Map.of("name", "jane"))
            .document("{\"$set\": {\"touched\": true}}")
            .build();

        var e = assertThrows(RunnableTaskException.class, () -> update.run(runContextFactory.of()));

        assertThat(e.getMessage(), containsString("may not be durable"));
        var output = (Update.Output) e.getOutput();
        assertThat(output, notNullValue());
        assertThat(output.getModifiedCount(), is(1L));
        assertThat(output.getWasAcknowledged(), is(true));
    }

    @Test
    void deleteKeepsItsCountOnAWriteConcernTimeout() throws Exception {
        var delete = Delete.builder()
            .connection(WriteConcernTimeoutConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(database()))
            .collection(Property.ofValue("wc"))
            .operation(Property.ofValue(Delete.Operation.DELETE_MANY))
            .filter(Map.of("name", "jane"))
            .build();

        var e = assertThrows(RunnableTaskException.class, () -> delete.run(runContextFactory.of()));

        assertThat(e.getMessage(), containsString("may not be durable"));
        var output = (Delete.Output) e.getOutput();
        assertThat(output, notNullValue());
        assertThat(output.getDeletedCount(), is(1L));
        assertThat(output.getWasAcknowledged(), is(true));
    }

    // a connection whose client throws a write-concern timeout on any single-document write, with the write reported applied
    @SuperBuilder
    private static class WriteConcernTimeoutConnection extends MongoDbConnection {
        private static final Set<String> WRITES = Set.of("insertOne", "updateOne", "updateMany", "replaceOne", "deleteOne", "deleteMany");

        @Override
        MongoClient client(RunContext runContext, CommandListener listener) throws IllegalVariableEvaluationException {
            return failWrite(super.client(runContext, listener), MongoClient.class);
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private static <T> T failWrite(T target, Class<T> type) {
            return type.cast(Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[] {type},
                (proxy, method, args) ->
                {
                    if (WRITES.contains(method.getName())) {
                        throw new MongoWriteConcernException(
                            new WriteConcernError(64, "WriteConcernFailed", "waiting for replication timed out", new BsonDocument()),
                            WriteConcernResult.acknowledged(1, true, null),
                            new ServerAddress()
                        );
                    }

                    Object result;
                    try {
                        result = method.invoke(target, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }

                    if (result instanceof MongoDatabase database) {
                        return failWrite(database, MongoDatabase.class);
                    }
                    if (result instanceof MongoCollection collection) {
                        return failWrite(collection, (Class) MongoCollection.class);
                    }
                    return result;
                }
            ));
        }
    }
}
