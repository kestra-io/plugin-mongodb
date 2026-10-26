package io.kestra.plugin.mongodb;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTaskException;
import io.kestra.core.utils.IdUtils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

// The shared container is a single-node replica set, so a w=2 write commits on the primary and then its write
// concern can never be met: the applied-but-not-durable condition, exercised against the real driver with no stubbing.
// InsertOne, Update and Delete must then FAIL but keep the outputs the driver can report, like AbstractLoad.
class WriteConcernTimeoutTest extends MongoDbContainer {
    private static MongoClient client;

    @BeforeAll
    static void openClient() {
        client = getMongoClient();
    }

    @AfterAll
    static void closeClient() {
        if (client != null) {
            client.close();
        }
    }

    private static String writeConcernUri() {
        return connectionUri + "/?w=2&wtimeoutMS=500";
    }

    private static MongoDbConnection connection() {
        return MongoDbConnection.builder().uri(Property.ofValue(writeConcernUri())).build();
    }

    private static String database() {
        return "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);
    }

    private MongoCollection<Document> collection(String database) {
        return client.getDatabase(database).getCollection("wc");
    }

    @Test
    void insertOneKeepsTheIdAndStaysOnThePrimary() throws Exception {
        String database = database();
        var insert = InsertOne.builder()
            .connection(connection())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("wc"))
            .document(Map.of("_id", "doc1", "name", "jane"))
            .build();

        var e = assertThrows(RunnableTaskException.class, () -> insert.run(runContextFactory.of()));

        assertThat(e.getMessage(), containsString("may not be durable"));
        var output = (InsertOne.Output) e.getOutput();
        assertThat(output, notNullValue());
        assertThat(output.getInsertedId(), is("doc1"));
        assertThat(output.getWasAcknowledged(), is(true));
        assertThat("the write is on the primary", collection(database).countDocuments(new Document("_id", "doc1")), is(1L));
    }

    @Test
    void updateReportsTheMatchedCountButNotAModifiedCount() throws Exception {
        String database = database();
        // two documents that already hold the value, so UPDATE_MANY matches both but modifies none
        collection(database).insertMany(List.of(new Document("_id", "u1").append("v", 1), new Document("_id", "u2").append("v", 1)));

        var update = Update.builder()
            .connection(connection())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("wc"))
            .operation(Property.ofValue(Update.Operation.UPDATE_MANY))
            .filter(Map.of("v", 1))
            .document("{\"$set\": {\"v\": 1}}")
            .build();

        var e = assertThrows(RunnableTaskException.class, () -> update.run(runContextFactory.of()));

        assertThat(e.getMessage(), containsString("may not be durable"));
        var output = (Update.Output) e.getOutput();
        assertThat(output, notNullValue());
        assertThat(output.getWasAcknowledged(), is(true));
        assertThat("both documents matched", output.getMatchedCount(), is(2L));
        // the driver carries no nModified on a write concern error, so the matched count must not be passed off as modified
        assertThat(output.getModifiedCount(), is(nullValue()));
    }

    @Test
    void deleteKeepsItsCountAndLeavesThePrimaryEmpty() throws Exception {
        String database = database();
        collection(database).insertOne(new Document("_id", "d1"));

        var delete = Delete.builder()
            .connection(connection())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("wc"))
            .operation(Property.ofValue(Delete.Operation.DELETE_ONE))
            .filter(Map.of("_id", "d1"))
            .build();

        var e = assertThrows(RunnableTaskException.class, () -> delete.run(runContextFactory.of()));

        assertThat(e.getMessage(), containsString("may not be durable"));
        var output = (Delete.Output) e.getOutput();
        assertThat(output, notNullValue());
        assertThat(output.getWasAcknowledged(), is(true));
        assertThat(output.getDeletedCount(), is(1L));
        assertThat("the delete is on the primary", collection(database).countDocuments(new Document("_id", "d1")), is(0L));
    }
}
