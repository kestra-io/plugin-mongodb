package io.kestra.plugin.mongodb;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
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
    void tiedDocumentsWithAnIdOfAnotherTypeAreNotSkippedWhenLimitSplitsThem() throws Exception {
        var objectId = new ObjectId();
        insert(order(1, 1), new Document("_id", "a").append("updatedAt", at(1)), new Document("_id", objectId).append("updatedAt", at(1)), order(2, 2));
        var trigger = trigger("updatedAt").limit(Property.ofValue(1)).build();

        assertThat(poll(trigger), contains(1));
        assertThat(poll(trigger), contains("a"));
        assertThat(poll(trigger), contains(Map.of("$oid", objectId.toHexString())));
        assertThat(poll(trigger), contains(2));
        assertThat(poll(trigger), empty());
    }

    @Test
    void tiedDocumentsWrittenLaterWithAnIdOfAnotherTypeAreDelivered() throws Exception {
        insert(new Document("_id", "k1").append("updatedAt", at(1)));
        var trigger = trigger("updatedAt").build();
        assertThat(poll(trigger), contains("k1"));

        var objectId = new ObjectId();
        insert(new Document("_id", objectId).append("updatedAt", at(1)), new Document("_id", "k2").append("updatedAt", at(1)));
        assertThat(poll(trigger), contains("k2", Map.of("$oid", objectId.toHexString())));
        assertThat(poll(trigger), empty());
    }

    @Test
    void tiedDocumentsWhoseIdLooksLikeAFieldPathAreDeliveredOnce() throws Exception {
        insert(
            new Document("_id", "$a").append("updatedAt", at(1)),
            new Document("_id", "$b").append("updatedAt", at(1)),
            new Document("_id", new Document("k", "$x")).append("updatedAt", at(1))
        );
        var trigger = trigger("updatedAt").limit(Property.ofValue(1)).build();

        assertThat(poll(trigger), contains("$a"));
        assertThat(poll(trigger), contains("$b"));
        assertThat(poll(trigger), contains(Map.of("k", "$x")));
        assertThat(poll(trigger), empty());
    }

    @Test
    void tiedDocumentsAreDeliveredInMongoDbOrderWhateverTheTypeOfTheirId() throws Exception {
        // inserted in the reverse of MongoDB's order between types, where NaN comes before the other numbers
        List<Object> ids = Arrays.asList(
            new MaxKey(), new Code("x"), new BsonTimestamp(1, 1), at(1), true, new ObjectId(), new Binary(new byte[] { 1 }),
            new Document("k", 1), new Symbol("b"), "a", Decimal128.parse("3.5"), 2.5, 2L, 1, Double.NaN, null, new MinKey()
        );
        insert(IntStream.range(0, ids.size()).mapToObj(i -> new Document("_id", ids.get(i)).append("updatedAt", at(1)).append("n", i)).toArray(Document[]::new));

        List<Object> mongoDbOrder;
        try (MongoClient client = MongoClients.create(connectionUri)) {
            mongoDbOrder = orders(client).find().sort(Sorts.ascending("updatedAt", "_id")).map(document -> document.get("n")).into(new ArrayList<>());
        }

        var trigger = trigger("updatedAt").limit(Property.ofValue(1)).build();
        var delivered = new ArrayList<>();
        for (int i = 0; i <= ids.size(); i++) {
            delivered.addAll(poll(trigger, "n"));
        }
        assertThat(delivered, is(mongoDbOrder));
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
    void objectIdWatermark() throws Exception {
        var first = new ObjectId();
        var second = new ObjectId();
        insert(new Document("_id", first), new Document("_id", second));
        var trigger = trigger("_id").build();

        assertThat(poll(trigger), contains(Map.of("$oid", first.toHexString()), Map.of("$oid", second.toHexString())));
        assertThat(storedPosition(trigger).orElseThrow().getObjectId("value").getValue(), is(second));
        assertThat(poll(trigger), empty());

        var third = new ObjectId();
        insert(new Document("_id", third));
        assertThat(poll(trigger), contains(Map.of("$oid", third.toHexString())));
    }

    @Test
    void stringWatermark() throws Exception {
        insert(new Document("_id", 1).append("updatedAt", isoAt(2)), new Document("_id", 2).append("updatedAt", isoAt(1)));
        var trigger = trigger("updatedAt").build();

        assertThat(poll(trigger), contains(2, 1));
        assertThat(storedPosition(trigger).orElseThrow().getString("value").getValue(), is(isoAt(2)));
        assertThat(poll(trigger), empty());

        insert(new Document("_id", 3).append("updatedAt", isoAt(3)));
        assertThat(poll(trigger), contains(3));
    }

    @Test
    void documentsOfAnotherTypeThanThePositionAreNeverReturned() throws Exception {
        insert(new Document("_id", 1).append("seq", 1));
        var trigger = trigger("seq").build();
        assertThat(poll(trigger), contains(1));

        // strings sort after numbers, but $gt only matches values of the position's type
        insert(new Document("_id", 2).append("seq", "2"), new Document("_id", 3).append("seq", 3));
        assertThat(poll(trigger), contains(3));
        assertThat(poll(trigger), empty());
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

        poll(trigger);

        var position = storedPosition(trigger).orElseThrow();
        assertThat(position.getString("field").getValue(), is("updatedAt"));
        assertThat(position.getDateTime("value").getValue(), is(at(2).getTime()));
        assertThat(position.getInt32("_id").getValue(), is(2));
    }

    @Test
    void positionIsNotSavedWhenTheExecutionCantBeGenerated() throws Exception {
        insert(order(1, 1));
        var trigger = trigger("updatedAt").build();
        var context = mockTrigger(trigger);
        // TriggerService can't generate an execution without the flow
        var withoutFlow = ConditionContext.builder().runContext(context.getKey().getRunContext()).build();

        assertThrows(NullPointerException.class, () -> trigger.evaluate(withoutFlow, context.getValue()));
        assertThat(storedPosition(trigger).isPresent(), is(false));
        assertThat(poll(trigger), contains(1));
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
        var context = mockTrigger(trigger);
        return trigger.evaluate(context.getKey(), context.getValue());
    }

    // TestsUtils.mockTrigger names the flow after its caller, so every call goes through here to share one flow and KV key
    private Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> mockTrigger(Trigger trigger) {
        return TestsUtils.mockTrigger(runContextFactory, trigger);
    }

    // the _id of each delivered row, in delivery order
    private List<Object> poll(Trigger trigger) throws Exception {
        return poll(trigger, "_id");
    }

    @SuppressWarnings("unchecked")
    private List<Object> poll(Trigger trigger, String field) throws Exception {
        return evaluate(trigger)
            .map(execution -> ((List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows")).stream().map(row -> row.get(field)).toList())
            .orElse(List.of());
    }

    private Optional<BsonDocument> storedPosition(Trigger trigger) throws Exception {
        var context = mockTrigger(trigger);
        var key = TriggerWatermark.key(context.getValue().getFlowId(), trigger.getId());
        return context.getKey().getRunContext().namespaceKv(context.getValue().getNamespace()).getValue(key)
            .map(stored -> BsonDocument.parse((String) stored.value()));
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

    private static String isoAt(int minute) {
        return at(minute).toInstant().toString();
    }
}
