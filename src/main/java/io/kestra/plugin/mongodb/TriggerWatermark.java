package io.kestra.plugin.mongodb;

import java.io.IOException;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import io.kestra.core.exceptions.ResourceExpiredException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.kv.KVMetadata;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVValueAndMetadata;

/**
 * Position of the last document a {@link Trigger} delivered, kept in the namespace KV Store so that each
 * document is delivered once.
 *
 * <p>
 * Documents are read in ({@code field}, {@code _id}) order and the position is the pair of the last one, so
 * documents sharing a field value are neither skipped nor delivered twice when {@code limit} splits them.
 * One instance serves a single poll.
 */
final class TriggerWatermark implements Find.QueryHook {
    private static final String ID = "_id";
    // JsonMode.EXTENDED writes canonical Extended JSON (the driver has no CANONICAL mode), which unlike RELAXED
    // keeps the BSON type of the stored value (date, ObjectId, Int64, ...)
    private static final JsonWriterSettings EXTENDED_JSON = JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();
    // the order MongoDB sorts values of different types in, see https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/
    private static final List<Set<BsonType>> TYPE_ORDER = List.of(
        EnumSet.of(BsonType.MIN_KEY),
        EnumSet.of(BsonType.NULL),
        EnumSet.of(BsonType.DOUBLE, BsonType.INT32, BsonType.INT64, BsonType.DECIMAL128),
        EnumSet.of(BsonType.STRING, BsonType.SYMBOL),
        EnumSet.of(BsonType.DOCUMENT),
        EnumSet.of(BsonType.ARRAY),
        EnumSet.of(BsonType.BINARY),
        EnumSet.of(BsonType.OBJECT_ID),
        EnumSet.of(BsonType.BOOLEAN),
        EnumSet.of(BsonType.DATE_TIME),
        EnumSet.of(BsonType.TIMESTAMP),
        EnumSet.of(BsonType.REGULAR_EXPRESSION),
        EnumSet.of(BsonType.DB_POINTER),
        EnumSet.of(BsonType.JAVASCRIPT),
        EnumSet.of(BsonType.JAVASCRIPT_WITH_SCOPE),
        EnumSet.of(BsonType.MAX_KEY)
    );

    private final KVStore kvStore;
    private final String key;
    private final String field;
    private final BsonValue lastValue;
    private final BsonValue lastId;
    private BsonDocument lastDocument;

    private TriggerWatermark(KVStore kvStore, String key, String field, BsonValue lastValue, BsonValue lastId) {
        this.kvStore = kvStore;
        this.key = key;
        this.field = field;
        this.lastValue = lastValue;
        this.lastId = lastId;
    }

    static String key(String flowId, String triggerId) {
        // the flow id is length-prefixed so that ("ab", "c") and ("a", "bc") can't share a key
        return "mongodb_trigger_watermark_" + flowId.length() + "_" + flowId + "_" + triggerId;
    }

    static TriggerWatermark load(RunContext runContext, String namespace, String flowId, String triggerId, String field) throws IOException, ResourceExpiredException {
        var kvStore = runContext.namespaceKv(namespace);
        var key = key(flowId, triggerId);

        var stored = kvStore.getValue(key);
        if (stored.isEmpty()) {
            return new TriggerWatermark(kvStore, key, field, null, null);
        }

        BsonDocument position;
        try {
            position = BsonDocument.parse((String) stored.get().value());
        } catch (RuntimeException e) {
            throw new IllegalStateException(unreadable(key, namespace), e);
        }

        if (!(position.get("field") instanceof BsonString storedField) || !position.containsKey("value")) {
            throw new IllegalStateException(unreadable(key, namespace));
        }

        if (!storedField.getValue().equals(field)) {
            runContext.logger().info(
                "Watermark field changed from '{}' to '{}', starting again from the first matching document",
                storedField.getValue(),
                field
            );
            return new TriggerWatermark(kvStore, key, field, null, null);
        }

        if (!field.equals(ID) && !position.containsKey(ID)) {
            throw new IllegalStateException(unreadable(key, namespace));
        }

        return new TriggerWatermark(kvStore, key, field, position.get("value"), position.get(ID));
    }

    private static String unreadable(String key, String namespace) {
        return "Could not read the MongoDB trigger watermark stored in the KV key '" + key + "' of namespace '" + namespace + "'. " +
            "Delete that KV entry to start again from the first matching document.";
    }

    @Override
    public BsonDocument filter(BsonDocument filter) {
        BsonDocument after;
        if (lastValue == null) {
            // documents without a value can't be ordered, so they are never delivered
            after = new BsonDocument(field, new BsonDocument("$ne", BsonNull.VALUE));
        } else if (field.equals(ID)) {
            after = new BsonDocument(ID, new BsonDocument("$gt", lastValue));
        } else {
            var branches = new BsonArray(
                List.of(
                    new BsonDocument(field, new BsonDocument("$gt", lastValue)),
                    new BsonDocument(field, lastValue).append(ID, new BsonDocument("$gt", lastId))
                )
            );
            // $gt only matches an _id of the position's type, so tied documents with an _id of a type sorted after it need $type
            var laterTypes = typesAfter(lastId.getBsonType());
            if (!laterTypes.isEmpty()) {
                branches.add(new BsonDocument(field, lastValue).append(ID, new BsonDocument("$type", laterTypes)));
            }
            after = new BsonDocument("$or", branches);
        }

        return filter.isEmpty() ? after : new BsonDocument("$and", new BsonArray(List.of(filter, after)));
    }

    // the $type numbers of the types MongoDB sorts after the given one; they equal BsonType's values except for MinKey, which sorts first
    private static BsonArray typesAfter(BsonType type) {
        var types = new BsonArray();
        var after = false;
        for (var group : TYPE_ORDER) {
            if (after) {
                group.forEach(later -> types.add(new BsonInt32(later.getValue())));
            }
            after = after || group.contains(type);
        }
        return types;
    }

    @Override
    public BsonDocument sort(BsonDocument sort) {
        var order = new BsonDocument(field, new BsonInt32(1));
        if (!field.equals(ID)) {
            order.append(ID, new BsonInt32(1));
        }
        return order;
    }

    @Override
    public void document(BsonDocument document) {
        this.lastDocument = document;
    }

    /**
     * Stores the position of the last returned document, if any.
     */
    void save() throws IOException {
        if (lastDocument == null) {
            return;
        }

        var value = valueAt(lastDocument, field);
        if (value == null) {
            throw new IllegalStateException("The returned documents don't contain the watermark field '" + field + "': include it in `projection`.");
        }
        if (value.isArray() || value.isDocument()) {
            throw new IllegalStateException(
                "The watermark field '" + field + "' must hold a single value such as a date, a number, a string or an ObjectId, not " + value.getBsonType() + "."
            );
        }

        var position = new BsonDocument("field", new BsonString(field)).append("value", value);
        if (!field.equals(ID)) {
            var id = lastDocument.get(ID);
            if (id == null) {
                throw new IllegalStateException("The returned documents don't contain `_id`, which the watermark needs: don't exclude it in `projection`.");
            }
            position.append(ID, id);
        }

        kvStore.put(
            key,
            new KVValueAndMetadata(
                new KVMetadata("Position of the last document delivered by the MongoDB trigger", (Duration) null),
                position.toJson(EXTENDED_JSON)
            )
        );
    }

    // follows a dotted path such as "meta.updatedAt" through nested documents
    private static BsonValue valueAt(BsonDocument document, String path) {
        BsonValue value = document;
        for (var part : path.split("\\.")) {
            if (!value.isDocument()) {
                return null;
            }
            value = value.asDocument().get(part);
            if (value == null) {
                return null;
            }
        }
        return value;
    }
}
