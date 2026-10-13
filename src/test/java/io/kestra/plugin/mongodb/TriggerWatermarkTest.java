package io.kestra.plugin.mongodb;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class TriggerWatermarkTest extends MongoDbContainer {
    private static final String DATABASE = "samples";

    private String collection;

    @BeforeEach
    void setUp() {
        collection = "watermark_" + IdUtils.create();
    }

    @Test
    void withoutWatermarkEveryPollDeliversTheSameDocuments() throws Exception {
        insert(order(1, 1), order(2, 2));
        var trigger = trigger(null).build();

        assertThat(poll(trigger), contains(1, 2));
        assertThat(poll(trigger), contains(1, 2));
    }

    @Test
    void eachNewOrUpdatedDocumentIsDeliveredOnce() throws Exception {
        insert(order(2, 2), order(1, 1));
        var trigger = trigger("updatedAt").build();

        assertThat(poll(trigger), contains(1, 2));
        assertThat(poll(trigger), empty());

        insert(order(3, 3));
        assertThat(poll(trigger), contains(3));

        try (MongoClient client = MongoClients.create(connectionUri)) {
            orders(client).updateOne(Filters.eq("_id", 1), Updates.set("updatedAt", at(4)));
        }
        assertThat(poll(trigger), contains(1));
        assertThat(poll(trigger), empty());
    }

    @Test
    void documentsSharingAValueAreNotSkippedWhenLimitSplitsThem() throws Exception {
        insert(order(3, 1), order(1, 1), order(2, 1));
        var trigger = trigger("updatedAt").limit(Property.ofValue(2)).build();

        assertThat(poll(trigger), contains(1, 2));
        assertThat(poll(trigger), contains(3));
        assertThat(poll(trigger), empty());
    }

    @Test
    void idWatermark() throws Exception {
        insert(new Document("_id", 1), new Document("_id", 2));
        var trigger = trigger("_id").build();

        assertThat(poll(trigger), contains(1, 2));
        assertThat(poll(trigger), empty());

        insert(new Document("_id", 3));
        assertThat(poll(trigger), contains(3));
    }

    @Test
    void nestedFieldAndDocumentsWithoutAValue() throws Exception {
        insert(
            new Document("_id", 1).append("meta", new Document("updatedAt", at(1))),
            new Document("_id", 2),
            new Document("_id", 3).append("meta", new Document("updatedAt", null))
        );
        var trigger = trigger("meta.updatedAt").build();

        assertThat(poll(trigger), contains(1));
        assertThat(poll(trigger), empty());
    }

    @Test
    void storedResultsAdvanceTheWatermark() throws Exception {
        insert(order(1, 1), order(2, 2));
        var trigger = trigger("updatedAt").store(Property.ofValue(true)).build();

        var first = evaluate(trigger);
        assertThat(first.isPresent(), is(true));
        assertThat(((Number) first.get().getTrigger().getVariables().get("size")).longValue(), is(2L));
        assertThat(first.get().getTrigger().getVariables().get("uri"), notNullValue());

        assertThat(evaluate(trigger).isPresent(), is(false));
    }

    @Test
    void positionIsStoredInTheNamespaceKvStore() throws Exception {
        insert(order(1, 1), order(2, 2));
        var trigger = trigger("updatedAt").build();
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        trigger.evaluate(context.getKey(), context.getValue());

        var key = TriggerWatermark.key(context.getValue().getFlowId(), trigger.getId());
        var stored = context.getKey().getRunContext().namespaceKv(context.getValue().getNamespace()).getValue(key);
        assertThat(stored.isPresent(), is(true));

        var position = BsonDocument.parse((String) stored.get().value());
        assertThat(position.getString("field").getValue(), is("updatedAt"));
        assertThat(position.getDateTime("value").getValue(), is(at(2).getTime()));
        assertThat(position.getInt32("_id").getValue(), is(2));
    }

    @Test
    void eachTriggerKeepsItsOwnPosition() throws Exception {
        insert(order(1, 1), order(2, 2));
        var first = trigger("updatedAt").build();
        var second = trigger("updatedAt").build();

        assertThat(poll(first), contains(1, 2));
        assertThat(poll(second), contains(1, 2));
        assertThat(poll(first), empty());
    }

    @Test
    void changingTheWatermarkFieldStartsAgain() throws Exception {
        insert(order(1, 1).append("seq", 10), order(2, 2).append("seq", 20));
        var id = "watch_" + IdUtils.create();

        assertThat(poll(trigger("updatedAt").id(id).build()), contains(1, 2));
        assertThat(poll(trigger("seq").id(id).build()), contains(1, 2));
        assertThat(poll(trigger("seq").id(id).build()), empty());
    }

    @Test
    void sortAndSkipCantBeCombinedWithAWatermark() {
        var sorted = trigger("updatedAt").sort(Map.of("updatedAt", -1)).build();
        var sortError = assertThrows(IllegalArgumentException.class, () -> evaluate(sorted));
        assertThat(sortError.getMessage(), containsString("`sort` can't be combined with `watermarkField`"));

        var skipped = trigger("updatedAt").skip(Property.ofValue(1)).build();
        var skipError = assertThrows(IllegalArgumentException.class, () -> evaluate(skipped));
        assertThat(skipError.getMessage(), containsString("`skip` can't be combined with `watermarkField`"));
    }

    private Trigger.TriggerBuilder<?, ?> trigger(String watermarkField) {
        var builder = Trigger.builder()
            .id("watch_" + IdUtils.create())
            .type(Trigger.class.getName())
            .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(collection));

        return watermarkField == null ? builder : builder.watermarkField(Property.ofValue(watermarkField));
    }

    private Optional<Execution> evaluate(Trigger trigger) throws Exception {
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        return trigger.evaluate(context.getKey(), context.getValue());
    }

    // the _id of each delivered row, in delivery order
    @SuppressWarnings("unchecked")
    private List<Object> poll(Trigger trigger) throws Exception {
        return evaluate(trigger)
            .map(execution -> ((List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows")).stream().map(row -> row.get("_id")).toList())
            .orElse(List.of());
    }

    private void insert(Document... documents) {
        try (MongoClient client = MongoClients.create(connectionUri)) {
            orders(client).insertMany(List.of(documents));
        }
    }

    private MongoCollection<Document> orders(MongoClient client) {
        return client.getDatabase(DATABASE).getCollection(collection);
    }

    private static Document order(int id, int minute) {
        return new Document("_id", id).append("updatedAt", at(minute));
    }

    private static Date at(int minute) {
        return Date.from(Instant.parse("2026-01-01T00:00:00Z").plusSeconds(60L * minute));
    }
}
