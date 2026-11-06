package io.kestra.plugin.mongodb;

import java.io.IOException;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;
import org.bson.BsonBinarySubType;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.json.JsonReader;
import org.bson.types.Decimal128;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;

public abstract class MongoDbService {
    /**
     * Maximum levels of nesting MongoDB accepts in a document.
     */
    public static final int MAX_NESTING_DEPTH = 100;

    /**
     * How an ObjectId is rendered in task output. HEX keeps the pre-existing bare hex string; EXTENDED_JSON
     * wraps it as {"$oid": "..."}, which Load decodes back to an ObjectId.
     */
    public enum ObjectIdFormat {
        HEX,
        EXTENDED_JSON
    }

    // ofJson() drops null map entries, so {deletedAt: null} becomes {} and matches every document.
    private static final ObjectMapper JSON_WITH_NULL_MAP_ENTRIES = JacksonMapper.ofJson().copy()
        .setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.ALWAYS));

    /**
     * Parses a JSON object into a document, failing first when it nests objects or arrays deeper than
     * {@code maxDepth}, so that the parser's recursion stays bounded on hostile input.
     */
    public static BsonDocument parse(String json, int maxDepth) {
        checkNestingDepth(json, maxDepth);
        return BsonDocument.parse(json);
    }

    public static BsonDocument parse(String json) {
        return parse(json, MAX_NESTING_DEPTH);
    }

    // walks the text with the driver's own reader, without recursion, so it counts what BsonDocument.parse will see
    private static void checkNestingDepth(String json, int maxDepth) {
        var reader = new JsonReader(json);
        var isDocument = new boolean[maxDepth + 1];
        var depth = 0;

        try {
            var type = reader.readBsonType();
            while (true) {
                if (type == BsonType.DOCUMENT || type == BsonType.ARRAY || type == BsonType.JAVASCRIPT_WITH_SCOPE) {
                    if (++depth > maxDepth) {
                        throw nestingTooDeep("The JSON value");
                    }
                    isDocument[depth] = type != BsonType.ARRAY;
                    if (type == BsonType.ARRAY) {
                        reader.readStartArray();
                    } else {
                        // skipValue() would recurse through the scope without a limit, so it is walked like a document
                        if (type == BsonType.JAVASCRIPT_WITH_SCOPE) {
                            reader.readJavaScriptWithScope();
                        }
                        reader.readStartDocument();
                    }
                } else {
                    reader.skipValue();
                }

                while (true) {
                    if (depth == 0) {
                        return;
                    }
                    type = reader.readBsonType();
                    if (type != BsonType.END_OF_DOCUMENT) {
                        break;
                    }
                    if (isDocument[depth]) {
                        reader.readEndDocument();
                    } else {
                        reader.readEndArray();
                    }
                    depth--;
                }
                if (isDocument[depth]) {
                    reader.readName();
                }
            }
        } catch (NestingTooDeepException e) {
            throw e;
        } catch (RuntimeException e) {
            // malformed JSON: BsonDocument.parse reports it with its own message, after at most maxDepth levels
        }
    }

    /**
     * Depth of a value, counting the value itself as one level when it is an object or an array. Only call it on
     * values that went through {@link #parse(String, int)}, whose depth is bounded.
     */
    public static int depth(BsonValue value) {
        if (value.isDocument()) {
            return 1 + value.asDocument().values().stream().mapToInt(MongoDbService::depth).max().orElse(0);
        } else if (value.isArray()) {
            return 1 + value.asArray().stream().mapToInt(MongoDbService::depth).max().orElse(0);
        } else if (value.isJavaScriptWithScope()) {
            return depth(value.asJavaScriptWithScope().getScope());
        }
        return 0;
    }

    public static void checkDepth(BsonValue value, String what) {
        if (depth(value) > MAX_NESTING_DEPTH) {
            throw nestingTooDeep(what);
        }
    }

    /**
     * Whether a deserialized value nests deeper than {@link #MAX_NESTING_DEPTH}. It stops descending at the limit,
     * so its recursion is bounded.
     */
    public static boolean exceedsMaxDepth(Object value, int level) {
        if (level > MAX_NESTING_DEPTH && (value instanceof Map || value instanceof List)) {
            return true;
        }
        if (value instanceof Map<?, ?> map) {
            return map.values().stream().anyMatch(v -> exceedsMaxDepth(v, level + 1));
        } else if (value instanceof List<?> list) {
            return list.stream().anyMatch(v -> exceedsMaxDepth(v, level + 1));
        }
        return false;
    }

    public static IllegalArgumentException nestingTooDeep(String what) {
        return new NestingTooDeepException(
            what + " is nested more than " + MAX_NESTING_DEPTH + " levels deep: MongoDB accepts at most " + MAX_NESTING_DEPTH
                + " levels of nesting in a document, flatten the data"
        );
    }

    private static class NestingTooDeepException extends IllegalArgumentException {
        NestingTooDeepException(String message) {
            super(message);
        }
    }

    @SuppressWarnings("unchecked")
    public static BsonDocument toDocument(RunContext runContext, Object value) throws IllegalVariableEvaluationException, IOException {
        if (value instanceof String) {
            return parse(runContext.render((String) value));
        } else if (value instanceof Map) {
            return toDocument(runContext.render((Map<String, Object>) value));
        } else if (value == null) {
            return new BsonDocument();
        } else {
            throw new IllegalVariableEvaluationException("Invalid value type '" + value.getClass() + "'");
        }
    }

    // for values that are already rendered: rendering them again would evaluate data as Pebble expressions
    public static BsonDocument toDocument(Map<String, Object> rendered) throws IOException {
        if (exceedsMaxDepth(rendered, 1)) {
            throw nestingTooDeep("The document");
        }

        return BsonDocument.parse(JSON_WITH_NULL_MAP_ENTRIES.writeValueAsString(rendered));
    }

    // add the _id before the driver does: with MongoCollection<Bson> it fails when the last field name written is a nested _id
    public static <T extends Map<String, ? super BsonObjectId>> T withId(T document) {
        if (document.get("_id") == null) {
            document.put("_id", new BsonObjectId());
        }

        return document;
    }

    static List<Map<String, Object>> readMapList(String json) throws IOException {
        return JSON_WITH_NULL_MAP_ENTRIES.readValue(json, new TypeReference<>() {
        });
    }

    // the write was applied on the primary but the requested write concern was not met; this is not always a
    // timeout, e.g. "Not enough data-bearing nodes" on a single-member replica set, so the wording stays neutral
    static String writeConcernErrorMessage(MongoWriteConcernException e) {
        return "MongoDB reported a write concern error; the write was applied on the primary but may not be durable: " + e.getMessage();
    }

    /**
     * Says why the outcome of a failed write is unknown, or returns null when the error is definite.
     * A failover or a network error during the write ends it without an answer, and a multi-document write
     * stopping at a failing document keeps the documents written before it; errors raised before the write was
     * sent wrote nothing.
     *
     * @param many whether the write touches several documents, so that it can stop part way
     */
    static String unknownWriteOutcome(MongoException e, boolean many) {
        // the driver labels a failure where it knows nothing was sent, such as after a failed retry
        if (e.hasErrorLabel("NoWritesPerformed") || e instanceof MongoSocketOpenException) {
            return null;
        }

        if (e instanceof MongoSocketException
            || e instanceof MongoOperationTimeoutException
            || e instanceof MongoNotPrimaryException
            || e instanceof MongoNodeIsRecoveringException) {
            // other MongoTimeoutExceptions come from server selection or the connection pool, before the write is sent
            return "before MongoDB confirmed it";
        }

        // a single-document write is atomic, so its write errors are definite
        if (many && e instanceof MongoWriteException) {
            return "when MongoDB stopped at a failing document";
        }

        return null;
    }

    /**
     * The error for a write whose outcome is unknown, keeping the driver error as its cause.
     *
     * @param unknown what is unknown, e.g. "it's unknown whether the document was updated"
     * @param retry what the user should know before retrying
     */
    static IllegalStateException unknownWriteOutcomeError(Enum<?> operation, String cause, String unknown, String retry, MongoException e) {
        return new IllegalStateException("The %s failed %s, so %s. %s: %s".formatted(operation, cause, unknown, retry, e.getMessage()), e);
    }

    public static String toIdString(BsonValue id) {
        switch (id.getBsonType()) {
            case OBJECT_ID:
                return id.asObjectId().getValue().toString();
            case STRING:
                return id.asString().getValue();
            case INT32:
                return String.valueOf(id.asInt32().getValue());
            case INT64:
                return String.valueOf(id.asInt64().getValue());
            case DOUBLE:
                return String.valueOf(id.asDouble().getValue());
            case DECIMAL128:
                return id.asDecimal128().getValue().toString();
            case BINARY: {
                var binary = id.asBinary();
                // legacy subtype 3 UUIDs use a different byte order in each driver, so only subtype 4 is decoded
                if (binary.getType() == BsonBinarySubType.UUID_STANDARD.getValue() && binary.getData().length == 16) {
                    return binary.asUuid().toString();
                }
                return toRelaxedJson(id);
            }
            default:
                return toRelaxedJson(id);
        }
    }

    // a stable form for any other type, e.g. {"a": 1} or {"$date": "2024-01-01T00:00:00Z"}
    private static String toRelaxedJson(BsonValue value) {
        // only a document serializes on its own, so the value is written as a field and the field's JSON is kept
        var json = new BsonDocument("v", value).toJson();
        return json.substring(json.indexOf(':') + 1, json.length() - 1).strip();
    }

    public static Object map(BsonValue doc, ObjectIdFormat objectIdFormat) {
        switch (doc.getBsonType()) {
            case NULL:
                return null;

            case INT32:
                return doc.asInt32().getValue();
            case INT64:
                return doc.asInt64().getValue();
            case DOUBLE:
                return doc.asDouble().getValue();
            case DECIMAL128:
                return decimal(doc.asDecimal128().getValue());

            case STRING:
                return doc.asString().getValue();

            case BOOLEAN:
                return doc.asBoolean().getValue();

            case DATE_TIME:
                return Instant.ofEpochMilli(doc.asDateTime().getValue());

            case DOCUMENT:
                return doc
                    .asDocument()
                    .entrySet()
                    .stream()
                    .map(
                        e -> new AbstractMap.SimpleEntry<>(
                            e.getKey(),
                            map(e.getValue(), objectIdFormat)
                        )
                    )
                    // https://bugs.openjdk.java.net/browse/JDK-8148463
                    .collect(LinkedHashMap::new, (m, v) -> m.put(v.getKey(), v.getValue()), LinkedHashMap::putAll);
            case ARRAY:
                return doc.asArray()
                    .stream()
                    .map(value -> map(value, objectIdFormat))
                    .collect(Collectors.toList());

            // deprecated types, read the way drivers usually decode them
            case SYMBOL:
                return doc.asSymbol().getSymbol();
            case UNDEFINED:
                return null;

            // no JSON equivalent: MongoDB Extended JSON, which Load writes back as the same BSON type
            case OBJECT_ID:
                // Load decodes $oid back to an ObjectId, a bare hex string loads as a String
                return objectIdFormat == ObjectIdFormat.EXTENDED_JSON
                    ? document("$oid", doc.asObjectId().getValue().toHexString())
                    : doc.asObjectId().getValue().toHexString();
            case BINARY: {
                var binary = doc.asBinary();
                if (binary.getType() == BsonBinarySubType.BINARY.getValue()) {
                    return binary.getData();
                }
                if (binary.getType() == BsonBinarySubType.UUID_STANDARD.getValue() && binary.getData().length == 16) {
                    return document("$uuid", binary.asUuid().toString());
                }
                return document(
                    "$binary",
                    document(
                        "base64", Base64.getEncoder().encodeToString(binary.getData()),
                        "subType", "%02x".formatted(binary.getType() & 0xFF)
                    )
                );
            }
            case TIMESTAMP:
                // seconds and increment are unsigned 32-bit values
                return document(
                    "$timestamp",
                    document(
                        "t", Integer.toUnsignedLong(doc.asTimestamp().getTime()),
                        "i", Integer.toUnsignedLong(doc.asTimestamp().getInc())
                    )
                );
            case REGULAR_EXPRESSION:
                return document(
                    "$regularExpression",
                    document("pattern", doc.asRegularExpression().getPattern(), "options", doc.asRegularExpression().getOptions())
                );
            case MIN_KEY:
                return document("$minKey", 1);
            case MAX_KEY:
                return document("$maxKey", 1);
            case JAVASCRIPT:
                return document("$code", doc.asJavaScript().getCode());
            case JAVASCRIPT_WITH_SCOPE:
                return document("$code", doc.asJavaScriptWithScope().getCode(), "$scope", map(doc.asJavaScriptWithScope().getScope(), objectIdFormat));
            case DB_POINTER:
                return document(
                    "$dbPointer",
                    document("$ref", doc.asDBPointer().getNamespace(), "$id", document("$oid", doc.asDBPointer().getId().toHexString()))
                );

            case END_OF_DOCUMENT:
            default:
                throw new IllegalArgumentException("Unsupported BsonValue " + doc.getBsonType() + ":" + doc);
        }
    }

    private static Object decimal(Decimal128 value) {
        try {
            // exact, where a Double keeps about 16 significant digits
            return value.bigDecimalValue();
        } catch (ArithmeticException e) {
            // NaN, infinities and negative zero have no BigDecimal form
            return value.doubleValue();
        }
    }

    // ordered, since Extended JSON readers such as Load recognize a wrapper by its first key
    private static Map<String, Object> document(String key, Object value) {
        var document = new LinkedHashMap<String, Object>();
        document.put(key, value);
        return document;
    }

    private static Map<String, Object> document(String key1, Object value1, String key2, Object value2) {
        var document = document(key1, value1);
        document.put(key2, value2);
        return document;
    }
}
