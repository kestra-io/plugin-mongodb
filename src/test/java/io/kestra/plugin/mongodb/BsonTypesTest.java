package io.kestra.plugin.mongodb;

import java.net.URI;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDbPointer;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonType;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoClient;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Reads one document holding the BSON types without a direct JSON equivalent through every read path, and pins the
 * value each path returns.
 */
@KestraTest
class BsonTypesTest extends MongoDbContainer {
    private static final String COLLECTION = "types";

    // JSON of the value every read path must return, with ' for "
    private static final Map<String, String> EXPECTED = expected();

    // ObjectId and date values inside a JavaScript scope
    private static final BsonDocument SCOPE = new BsonDocument("id", new BsonObjectId(new ObjectId("60930c39a982931c20ef6cd6")))
        .append("at", new BsonDateTime(Instant.parse("2024-01-01T10:00:00Z").toEpochMilli()));

    private String database;

    @BeforeEach
    void setUp() {
        this.database = "bson_types_" + IdUtils.create().toLowerCase(Locale.ROOT);
        try (MongoClient client = getMongoClient()) {
            client.getDatabase(this.database).getCollection(COLLECTION, BsonDocument.class).insertOne(
                new BsonDocument("_id", new BsonInt32(1))
                    .append("timestamp", new BsonTimestamp(1_700_000_000, 7))
                    .append("timestampAfter2038", new BsonTimestamp((int) 3_000_000_000L, 1))
                    .append("decimal", new BsonDecimal128(Decimal128.parse("1234567890123456789012345678901234")))
                    .append("decimalFraction", new BsonDecimal128(Decimal128.parse("0.10")))
                    .append("decimalNaN", new BsonDecimal128(Decimal128.NaN))
                    .append("decimalNegativeZero", new BsonDecimal128(Decimal128.NEGATIVE_ZERO))
                    .append("decimalInfinity", new BsonDecimal128(Decimal128.POSITIVE_INFINITY))
                    .append("regex", new BsonRegularExpression("^ab+c$", "i"))
                    .append("minKey", new BsonMinKey())
                    .append("maxKey", new BsonMaxKey())
                    .append("code", new BsonJavaScript("function(){return 1;}"))
                    .append("codeWithScope", new BsonJavaScriptWithScope("function(){return x;}", new BsonDocument("x", new BsonInt32(1))))
                    .append("codeWithScopeValues", new BsonJavaScriptWithScope("function(){return id;}", SCOPE))
                    .append("dbPointer", new BsonDbPointer("db.coll", new ObjectId("60930c39a982931c20ef6cd6")))
                    .append("oid", new BsonObjectId(new ObjectId("60930c39a982931c20ef6cd6")))
                    .append("symbol", new BsonSymbol("sym"))
                    .append("undefined", new BsonUndefined())
                    .append("nested", new BsonDocument("timestamp", new BsonTimestamp(1_700_000_000, 7)).append("decimal", new BsonDecimal128(Decimal128.parse("0.10"))))
                    .append("array", new BsonArray(List.of(new BsonTimestamp(1_700_000_000, 7), new BsonRegularExpression("^ab+c$", "i"))))
            );
        }
    }

    @Test
    void findReturnsTheSameValuesOnEveryPath() throws Exception {
        var fetched = find(false);
        assertRow("Find rows", fetched.getRows().getFirst());
        assertRow("Find outputs", ((List<?>) fetched.toMap().get("rows")).getFirst());

        var runContext = runContextFactory.of();
        assertRow("Find store", stored(runContext, find(runContext, true).getUri()));
    }

    @Test
    void aggregateReturnsTheSameValuesOnEveryPath() throws Exception {
        var runContext = runContextFactory.of();
        assertRow("Aggregate rows", aggregate(runContext, FetchType.FETCH).getRows().getFirst());
        assertRow("Aggregate store", stored(runContext, aggregate(runContext, FetchType.STORE).getUri()));
    }

    @Test
    void triggerReturnsTheSameValues() throws Exception {
        var trigger = Trigger.builder()
            .id("watch")
            .type(Trigger.class.getName())
            .connection(connection())
            .database(Property.ofValue(this.database))
            .collection(Property.ofValue(COLLECTION))
            .build();
        var flow = Flow.builder()
            .id("bson-types")
            .namespace("io.kestra.tests")
            .tenantId(TenantService.MAIN_TENANT)
            .revision(1)
            .triggers(List.of(trigger))
            .build();
        var triggerContext = TriggerContext.builder()
            .tenantId(TenantService.MAIN_TENANT)
            .namespace("io.kestra.tests")
            .flowId("bson-types")
            .triggerId("watch")
            .date(ZonedDateTime.now())
            .build();
        RunContext runContext = runContextFactory.initializer()
            .forScheduler((DefaultRunContext) runContextFactory.of(flow, trigger), triggerContext, trigger);
        var conditionContext = ConditionContext.builder().flow(flow).runContext(runContext).build();

        var execution = trigger.evaluate(conditionContext, triggerContext).orElseThrow();

        assertRow("Trigger", ((List<?>) execution.getTrigger().getVariables().get("rows")).getFirst());
    }

    @Test
    void loadWritesTheExtendedJsonBackAsTheSameType() throws Exception {
        var runContext = runContextFactory.of();
        var stored = find(runContext, true).getUri();

        Load.builder()
            .id(IdUtils.create())
            .type(Load.class.getName())
            .connection(connection())
            .database(Property.ofValue(this.database))
            .collection(Property.ofValue("copy"))
            .from(Property.ofValue(stored.toString()))
            .build()
            .run(runContext);

        try (MongoClient client = getMongoClient()) {
            var original = client.getDatabase(this.database).getCollection(COLLECTION, BsonDocument.class).find().first();
            var copy = client.getDatabase(this.database).getCollection("copy", BsonDocument.class).find().first();
            for (var field : List.of("timestamp", "timestampAfter2038", "decimal", "decimalFraction", "regex", "minKey", "maxKey", "code", "codeWithScope", "dbPointer", "oid", "nested", "array")) {
                assertThat(field, copy.get(field), is(original.get(field)));
            }

            // an ObjectId inside a JavaScript scope round-trips as an ObjectId now that map() wraps it; the Instant
            // in the same scope still comes back as a string, since map() reads a date as an Instant, not a wrapper
            assertThat(
                copy.get("codeWithScopeValues").asJavaScriptWithScope().getScope(),
                is(new BsonDocument("id", new BsonObjectId(new ObjectId("60930c39a982931c20ef6cd6"))).append("at", new BsonString("2024-01-01T10:00:00Z")))
            );
        }
    }

    /**
     * The Find with store: true then Load copy path used to rewrite every ObjectId to a plain string, so the copy
     * stopped matching the source under BSON type-aware equality. The stored row now carries $oid wrappers, and the
     * copy keeps the ObjectIds, including references, nested documents and array elements.
     */
    @Test
    void findStoreThenLoadKeepsObjectIds() throws Exception {
        var runContext = runContextFactory.of();
        var id = new ObjectId();
        var ref = new ObjectId();
        var child = new ObjectId();
        var inArray = new ObjectId();

        try (MongoClient client = getMongoClient()) {
            client.getDatabase(this.database).getCollection("objectid_src", BsonDocument.class).insertOne(
                new BsonDocument("_id", new BsonObjectId(id))
                    .append("ref", new BsonObjectId(ref))
                    .append("nested", new BsonDocument("child", new BsonObjectId(child)))
                    .append("arr", new BsonArray(List.of(new BsonInt32(1), new BsonObjectId(inArray))))
                    .append("when", new BsonDateTime(Instant.parse("2024-01-01T10:00:00Z").toEpochMilli()))
                    .append("n", new BsonInt64(9007199254740993L))
            );
        }

        var stored = Find.builder()
            .id(IdUtils.create())
            .type(Find.class.getName())
            .connection(connection())
            .database(Property.ofValue(this.database))
            .collection(Property.ofValue("objectid_src"))
            .store(Property.ofValue(true))
            .build()
            .run(runContext)
            .getUri();

        // the stored row pins the mapped form: ObjectIds are $oid wrappers, not bare hex strings
        var row = (Map<?, ?>) stored(runContext, stored);
        assertThat(row.get("_id"), is(Map.of("$oid", id.toHexString())));
        assertThat(row.get("ref"), is(Map.of("$oid", ref.toHexString())));
        assertThat(((Map<?, ?>) row.get("nested")).get("child"), is(Map.of("$oid", child.toHexString())));
        assertThat(((List<?>) row.get("arr")).get(1), is(Map.of("$oid", inArray.toHexString())));

        Load.builder()
            .id(IdUtils.create())
            .type(Load.class.getName())
            .connection(connection())
            .database(Property.ofValue(this.database))
            .collection(Property.ofValue("objectid_dst"))
            .from(Property.ofValue(stored.toString()))
            .build()
            .run(runContext);

        try (MongoClient client = getMongoClient()) {
            var dst = client.getDatabase(this.database).getCollection("objectid_dst", BsonDocument.class);
            var copy = dst.find().first();
            assertThat(copy.getObjectId("_id").getValue(), is(id));
            assertThat(copy.getObjectId("ref").getValue(), is(ref));
            assertThat(copy.getDocument("nested").getObjectId("child").getValue(), is(child));
            assertThat(copy.getArray("arr").get(1).asObjectId().getValue(), is(inArray));
            assertThat(copy.get("n").getBsonType(), is(BsonType.INT64));
            assertThat(copy.get("when").getBsonType(), is(BsonType.DATE_TIME));

            // an ObjectId equality filter matches the copy, which is what the lossy round trip broke
            assertThat(dst.countDocuments(new BsonDocument("_id", new BsonObjectId(id))), is(1L));
        }
    }

    private static Map<String, String> expected() {
        var expected = new LinkedHashMap<String, String>();
        expected.put("timestamp", "{'$timestamp':{'t':1700000000,'i':7}}");
        expected.put("timestampAfter2038", "{'$timestamp':{'t':3000000000,'i':1}}");
        expected.put("decimal", "1234567890123456789012345678901234");
        expected.put("decimalFraction", "0.10");
        expected.put("decimalNaN", "'NaN'");
        expected.put("decimalNegativeZero", "-0.0");
        expected.put("decimalInfinity", "'Infinity'");
        expected.put("regex", "{'$regularExpression':{'pattern':'^ab+c$','options':'i'}}");
        expected.put("minKey", "{'$minKey':1}");
        expected.put("maxKey", "{'$maxKey':1}");
        expected.put("code", "{'$code':'function(){return 1;}'}");
        expected.put("codeWithScope", "{'$code':'function(){return x;}','$scope':{'x':1}}");
        expected.put("codeWithScopeValues", "{'$code':'function(){return id;}','$scope':{'id':{'$oid':'60930c39a982931c20ef6cd6'},'at':'2024-01-01T10:00:00Z'}}");
        expected.put("dbPointer", "{'$dbPointer':{'$ref':'db.coll','$id':{'$oid':'60930c39a982931c20ef6cd6'}}}");
        expected.put("oid", "{'$oid':'60930c39a982931c20ef6cd6'}");
        expected.put("symbol", "'sym'");
        expected.put("undefined", "null");
        expected.put("nested", "{'timestamp':{'$timestamp':{'t':1700000000,'i':7}},'decimal':0.10}");
        expected.put("array", "[{'$timestamp':{'t':1700000000,'i':7}},{'$regularExpression':{'pattern':'^ab+c$','options':'i'}}]");
        return expected;
    }

    private static void assertRow(String path, Object row) throws Exception {
        var values = (Map<?, ?>) row;
        for (var entry : EXPECTED.entrySet()) {
            assertThat(
                path + " " + entry.getKey(),
                JacksonMapper.ofJson().writeValueAsString(values.get(entry.getKey())),
                is(entry.getValue().replace('\'', '"'))
            );
        }
    }

    private Object stored(RunContext runContext, URI uri) throws Exception {
        try (var inputStream = runContext.storage().getFile(uri)) {
            return FileSerde.readAll(inputStream).blockFirst();
        }
    }

    private Find.Output find(boolean store) throws Exception {
        return find(runContextFactory.of(), store);
    }

    private Find.Output find(RunContext runContext, boolean store) throws Exception {
        return Find.builder()
            .id(IdUtils.create())
            .type(Find.class.getName())
            .connection(connection())
            .database(Property.ofValue(this.database))
            .collection(Property.ofValue(COLLECTION))
            .store(Property.ofValue(store))
            .build()
            .run(runContext);
    }

    private Aggregate.Output aggregate(RunContext runContext, FetchType store) throws Exception {
        return Aggregate.builder()
            .id(IdUtils.create())
            .type(Aggregate.class.getName())
            .connection(connection())
            .database(Property.ofValue(this.database))
            .collection(Property.ofValue(COLLECTION))
            .pipeline(Property.ofValue(List.of(Map.of("$match", Map.of()))))
            .store(Property.ofValue(store))
            .build()
            .run(runContext);
    }

    private static MongoDbConnection connection() {
        return MongoDbConnection.builder()
            .uri(Property.ofValue(connectionUri))
            .build();
    }
}
