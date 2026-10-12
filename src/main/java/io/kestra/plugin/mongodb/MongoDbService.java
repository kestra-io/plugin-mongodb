package io.kestra.plugin.mongodb;

import java.io.IOException;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.types.Decimal128;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;

public abstract class MongoDbService {
    @SuppressWarnings("unchecked")
    public static BsonDocument toDocument(RunContext runContext, Object value) throws IllegalVariableEvaluationException, IOException {
        if (value instanceof String) {
            return BsonDocument.parse(runContext.render((String) value));
        } else if (value instanceof Map) {
            return BsonDocument.parse(JacksonMapper.ofJson().writeValueAsString(runContext.render((Map<String, Object>) value)));
        } else if (value == null) {
            return new BsonDocument();
        } else {
            throw new IllegalVariableEvaluationException("Invalid value type '" + value.getClass() + "'");
        }
    }

    public static Object map(BsonValue doc) {
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
            case BINARY:
                return doc.asBinary().getData();

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
                            map(e.getValue())
                        )
                    )
                    // https://bugs.openjdk.java.net/browse/JDK-8148463
                    .collect(LinkedHashMap::new, (m, v) -> m.put(v.getKey(), v.getValue()), LinkedHashMap::putAll);
            case ARRAY:
                return doc.asArray()
                    .stream()
                    .map(MongoDbService::map)
                    .collect(Collectors.toList());

            case OBJECT_ID:
                return doc.asObjectId().getValue().toString();

            // deprecated types, read the way drivers usually decode them
            case SYMBOL:
                return doc.asSymbol().getSymbol();
            case UNDEFINED:
                return null;

            // no JSON equivalent: MongoDB Extended JSON, which Load writes back as the same BSON type
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
                return document("$code", doc.asJavaScriptWithScope().getCode(), "$scope", map(doc.asJavaScriptWithScope().getScope()));
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
