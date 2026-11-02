package io.kestra.plugin.mongodb;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import com.mongodb.MongoException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.event.CommandListener;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;

import lombok.experimental.SuperBuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UpdateTest extends MongoDbContainer {

    @Test
    void writeWithoutAnAnswerHasAnUnknownCount() {
        var runContext = runContextFactory.of();
        var failure = new MongoSocketReadException("connection reset", new ServerAddress());

        var e = assertThrows(IllegalStateException.class, () -> failingUpdate(failure, Update.Operation.UPDATE_MANY).run(runContext));

        assertThat(
            e.getMessage(),
            is("The UPDATE_MANY failed before MongoDB confirmed it, so it's unknown how many documents were updated; some may have been modified. "
                + "Check that the update is safe to apply twice before retrying (see the task's Failures section): connection reset")
        );
        assertThat(e.getCause(), sameInstance(failure));
        assertThat(runContext.metrics(), empty());
    }

    @Test
    void writeOfOneDocumentWithoutAnAnswerSaysTheDocumentIsUnknown() {
        var failure = new MongoSocketReadException("connection reset", new ServerAddress());

        var e = assertThrows(IllegalStateException.class, () -> failingUpdate(failure, Update.Operation.UPDATE_ONE).run(runContextFactory.of()));

        assertThat(e.getMessage().startsWith("The UPDATE_ONE failed before MongoDB confirmed it, so it's unknown whether the document was updated."), is(true));
    }

    @Test
    void errorsBeforeTheWriteIsSentAreRethrownUnchanged() {
        var labelled = new MongoSocketReadException("connection reset", new ServerAddress());
        labelled.addLabel("NoWritesPerformed");

        for (MongoException failure : List.of(
            new MongoTimeoutException("Timed out while waiting for a server"),
            new MongoSocketOpenException("connection refused", new ServerAddress()),
            labelled
        )) {
            var runContext = runContextFactory.of();

            var e = assertThrows(MongoException.class, () -> failingUpdate(failure, Update.Operation.UPDATE_MANY).run(runContext));

            assertThat(e, sameInstance(failure));
            assertThat(runContext.metrics(), empty());
        }
    }

    @Test
    void updateManyStoppingAtAFailingDocumentHasAnUnknownCount() {
        var database = newDatabase();
        MongoCollection<Document> collection = getMongoClient().getDatabase(database).getCollection("unique");
        collection.createIndex(Indexes.ascending("code"), new IndexOptions().unique(true).sparse(true));
        collection.insertMany(List.of(new Document("_id", 1), new Document("_id", 2), new Document("_id", 3)));

        var e = assertThrows(IllegalStateException.class, () -> update(database, Update.Operation.UPDATE_MANY).run(runContextFactory.of()));

        assertThat(
            e.getMessage().startsWith("The UPDATE_MANY failed when MongoDB stopped at a failing document, so it's unknown how many documents were updated"),
            is(true)
        );
        assertThat(e.getCause() instanceof MongoWriteException, is(true));
        // the first document was updated before the duplicate key stopped the write
        assertThat(collection.countDocuments(new Document("code", "same")), is(1L));
    }

    @Test
    void updateOneWriteErrorIsDefinite() {
        var database = newDatabase();
        MongoCollection<Document> collection = getMongoClient().getDatabase(database).getCollection("unique");
        collection.createIndex(Indexes.ascending("code"), new IndexOptions().unique(true).sparse(true));
        collection.insertMany(List.of(new Document("_id", 1).append("code", "same"), new Document("_id", 2)));

        var task = Update.builder()
            .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("unique"))
            .operation(Property.ofValue(Update.Operation.UPDATE_ONE))
            .filter(Map.of("_id", 2))
            .document(Map.of("$set", Map.of("code", "same")))
            .build();

        assertThrows(MongoWriteException.class, () -> task.run(runContextFactory.of()));
    }

    private Update update(String database, Update.Operation operation) {
        return Update.builder()
            .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("unique"))
            .operation(Property.ofValue(operation))
            .filter(Map.of())
            .document(Map.of("$set", Map.of("code", "same")))
            .build();
    }

    private Update failingUpdate(MongoException failure, Update.Operation operation) {
        return Update.builder()
            .connection(FailingUpdateConnection.builder().uri(Property.ofValue(connectionUri)).failure(failure).build())
            .database(Property.ofValue(newDatabase()))
            .collection(Property.ofValue("failing"))
            .operation(Property.ofValue(operation))
            .filter(Map.of())
            .document(Map.of("$set", Map.of("done", true)))
            .build();
    }

    private static String newDatabase() {
        return "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);
    }

    // throws the given error from every update call; overriding Update would register a plugin
    @SuperBuilder
    private static class FailingUpdateConnection extends MongoDbConnection {
        private final MongoException failure;

        @Override
        MongoClient client(RunContext runContext, CommandListener listener) throws IllegalVariableEvaluationException {
            return failUpdates(super.client(runContext, listener), MongoClient.class, failure);
        }

        // proxies the client, and the databases and collections it returns, down to the update calls
        @SuppressWarnings({"unchecked", "rawtypes"})
        private static <T> T failUpdates(T target, Class<T> type, MongoException failure) {
            return type.cast(Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[] {type},
                (proxy, method, args) ->
                {
                    if (List.of("updateOne", "updateMany", "replaceOne").contains(method.getName())) {
                        throw failure;
                    }

                    Object result;
                    try {
                        result = method.invoke(target, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }

                    if (result instanceof MongoDatabase database) {
                        return failUpdates(database, MongoDatabase.class, failure);
                    }
                    if (result instanceof MongoCollection collection) {
                        return failUpdates(collection, (Class) MongoCollection.class, failure);
                    }
                    return result;
                }
            ));
        }
    }
}
