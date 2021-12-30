package io.kestra.plugin.mongodb;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.io.IOException;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;

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
                    .map(e -> new AbstractMap.SimpleEntry<>(
                        e.getKey(),
                        map(e.getValue())
                    ))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
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
