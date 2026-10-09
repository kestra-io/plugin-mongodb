package io.kestra.plugin.mongodb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonDocument;
import org.bson.BsonValue;

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
                BsonBinary binary = id.asBinary();
                if (BsonBinarySubType.isUuid(binary.getType()) && binary.getData().length == 16) {
                    // subtype 3 has no agreed byte order, so both UUID subtypes are read as stored
                    ByteBuffer bytes = ByteBuffer.wrap(binary.getData());
                    return new UUID(bytes.getLong(), bytes.getLong()).toString();
                }
                return id.toString();
            }
            default:
                return id.toString();
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
                return doc.asDecimal128().getValue();

            case STRING:
                return doc.asString().getValue();
            case BINARY:
                return doc.asBinary().getData();

            case BOOLEAN:
                return doc.asBoolean().getValue();

            case TIMESTAMP:
                return Instant.ofEpochMilli(doc.asTimestamp().getValue());
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

            case UNDEFINED:
                throw new IllegalArgumentException("Undefined BsonValue:" + doc);
            case MIN_KEY:
            case MAX_KEY:
            case JAVASCRIPT:
            case JAVASCRIPT_WITH_SCOPE:
            case REGULAR_EXPRESSION:
            case SYMBOL:
            case DB_POINTER:
            case END_OF_DOCUMENT:
            default:
                throw new IllegalArgumentException("Unsupported BsonValue " + doc.getBsonType() + ":" + doc);
        }
    }
}
