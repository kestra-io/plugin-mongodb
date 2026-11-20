package io.kestra.plugin.mongodb;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.mongodb.MongoException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InsertOneTest extends MongoDbContainer {

    @Test
    void insertWithoutAnAnswerHasAnUnknownOutcome() {
        var runContext = runContextFactory.of(Map.of());
        var failure = new MongoSocketReadException("connection reset", new ServerAddress());

        var e = assertThrows(IllegalStateException.class, () -> failingInsert(failure, Map.of("_id", "doc1", "name", "jane")).run(runContext));

        assertThat(
            e.getMessage(),
            is("The insert failed before MongoDB confirmed it, so it's unknown whether the document with `_id` doc1 was inserted. "
                + "Check whether it exists before retrying: a retry of a document without its own `_id` inserts a second copy "
                + "(see the task's Failures section): connection reset")
        );
        assertThat(e.getCause(), sameInstance(failure));
        assertThat(runContext.metrics(), empty());
    }

    @Test
    void insertWithoutAnAnswerNamesTheGeneratedId() {
        var failure = new MongoSocketReadException("connection reset", new ServerAddress());

        var e = assertThrows(IllegalStateException.class, () -> failingInsert(failure, Map.of("name", "jane")).run(runContextFactory.of(Map.of())));

        // the _id is set client-side before the insert is sent, so the error can name the ObjectId to look for
        assertThat(e.getMessage(), matchesPattern("(?s).*the document with `_id` [0-9a-f]{24} was inserted.*"));
    }

    @Test
    void insertErrorsBeforeTheWriteIsSentAreRethrownUnchanged() {
        var labelled = new MongoSocketReadException("connection reset", new ServerAddress());
        labelled.addLabel("NoWritesPerformed");

        for (MongoException failure : List.of(
            new MongoTimeoutException("Timed out while waiting for a server"),
            new MongoSocketOpenException("connection refused", new ServerAddress()),
            labelled
        )) {
            var e = assertThrows(MongoException.class, () -> failingInsert(failure, Map.of("_id", "doc1")).run(runContextFactory.of(Map.of())));

            assertThat(e, sameInstance(failure));
        }
    }

    @Test
    void insertDuplicateKeyIsDefinite() throws Exception {
        var database = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);
        insert(database, Map.of("_id", "dup")).run(runContextFactory.of(Map.of()));

        // a single-document write error means nothing was written, so it is not reported as an unknown outcome
        var e = assertThrows(MongoWriteException.class, () -> insert(database, Map.of("_id", "dup")).run(runContextFactory.of(Map.of())));

        assertThat(e.getCode(), is(11000));
    }

    private InsertOne failingInsert(MongoException failure, Map<String, Object> document) {
        return InsertOne.builder()
            .connection(FailingWriteConnection.builder().uri(Property.ofValue(connectionUri)).failure(failure).build())
            .database(Property.ofValue("ut_" + IdUtils.create().toLowerCase(Locale.ROOT)))
            .collection(Property.ofValue("insert"))
            .document(document)
            .build();
    }

    private InsertOne insert(String database, Map<String, Object> document) {
        return InsertOne.builder()
            .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("insert"))
            .document(document)
            .build();
    }
}
