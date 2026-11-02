package io.kestra.plugin.mongodb;

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
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;

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
        try (var client = getMongoClient()) {
            MongoCollection<Document> collection = client.getDatabase(database).getCollection("unique");
            collection.createIndex(Indexes.ascending("code"), new IndexOptions().unique(true).sparse(true));
            collection.insertMany(List.of(new Document("_id", 1), new Document("_id", 2), new Document("_id", 3)));
        }

        var e = assertThrows(IllegalStateException.class, () -> update(database, Update.Operation.UPDATE_MANY).run(runContextFactory.of()));

        assertThat(
            e.getMessage().startsWith("The UPDATE_MANY failed when MongoDB stopped at a failing document, so it's unknown how many documents were updated"),
            is(true)
        );
        assertThat(e.getCause() instanceof MongoWriteException, is(true));
        // the first document was updated before the duplicate key stopped the write
        try (var client = getMongoClient()) {
            assertThat(client.getDatabase(database).getCollection("unique").countDocuments(new Document("code", "same")), is(1L));
        }
    }

    @Test
    void updateOneWriteErrorIsDefinite() {
        var database = newDatabase();
        try (var client = getMongoClient()) {
            MongoCollection<Document> collection = client.getDatabase(database).getCollection("unique");
            collection.createIndex(Indexes.ascending("code"), new IndexOptions().unique(true).sparse(true));
            collection.insertMany(List.of(new Document("_id", 1).append("code", "same"), new Document("_id", 2)));
        }

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
            .connection(FailingWriteConnection.builder().uri(Property.ofValue(connectionUri)).failure(failure).build())
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
}
