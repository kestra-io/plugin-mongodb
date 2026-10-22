package io.kestra.plugin.mongodb;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoClient;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DeleteTest extends MongoDbContainer {

    @Test
    void deleteManyWithoutFilterFails() throws Exception {
        var database = seed();

        var delete = delete(database).operation(Property.ofValue(Delete.Operation.DELETE_MANY)).build();

        var error = assertThrows(IllegalArgumentException.class, () -> delete.run(runContextFactory.of(Map.of())));
        assertThat(error.getMessage(), containsString("`filter` is required with DELETE_MANY"));
        assertThat(count(database), is(3L));
    }

    @Test
    void deleteManyWithoutFilterFailsWhenOperationIsAnExpression() throws Exception {
        var database = seed();

        var delete = delete(database).operation(Property.ofExpression("{{ operation }}")).build();

        assertThrows(IllegalArgumentException.class, () -> delete.run(runContextFactory.of(Map.of("operation", "DELETE_MANY"))));
        assertThat(count(database), is(3L));
    }

    @Test
    void deleteManyWithEmptyMapFilterFails() throws Exception {
        var database = seed();

        var delete = delete(database)
            .operation(Property.ofValue(Delete.Operation.DELETE_MANY))
            .filter(Map.of())
            .build();

        var error = assertThrows(IllegalArgumentException.class, () -> delete.run(runContextFactory.of(Map.of())));
        assertThat(error.getMessage(), containsString("`filter` must not be an empty map with DELETE_MANY"));
        assertThat(count(database), is(3L));
    }

    @Test
    void deleteManyWithEmptyStringFilterDeletesEverything() throws Exception {
        var database = seed();

        var delete = delete(database)
            .operation(Property.ofValue(Delete.Operation.DELETE_MANY))
            .filter("{}")
            .build();

        assertThat(delete.run(runContextFactory.of(Map.of())).getDeletedCount(), is(3L));
        assertThat(count(database), is(0L));
    }

    @Test
    void deleteManyWithFilterDeletesOnlyMatches() throws Exception {
        var database = seed();

        var delete = delete(database)
            .operation(Property.ofValue(Delete.Operation.DELETE_MANY))
            .filter(Map.of("status", "old"))
            .build();

        assertThat(delete.run(runContextFactory.of(Map.of())).getDeletedCount(), is(2L));
        assertThat(count(database), is(1L));
    }

    @Test
    void deleteOneWithoutFilterIsUnchanged() throws Exception {
        var database = seed();

        var delete = delete(database).build();

        assertThat(delete.run(runContextFactory.of(Map.of())).getDeletedCount(), is(1L));
        assertThat(count(database), is(2L));
    }

    private Delete.DeleteBuilder<?, ?> delete(String database) {
        return Delete.builder()
            .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("delete"));
    }

    private String seed() {
        var database = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        try (MongoClient client = getMongoClient()) {
            client.getDatabase(database).getCollection("delete").insertMany(List.of(
                new Document("status", "old"),
                new Document("status", "old"),
                new Document("status", "new")
            ));
        }

        return database;
    }

    private long count(String database) {
        try (MongoClient client = getMongoClient()) {
            return client.getDatabase(database).getCollection("delete").countDocuments();
        }
    }
}
