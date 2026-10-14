package io.kestra.plugin.mongodb;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;
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
            after = new BsonDocument(
                "$or",
                new BsonArray(
                    List.of(
                        new BsonDocument(field, new BsonDocument("$gt", lastValue)),
                        new BsonDocument(field, lastValue).append(ID, new BsonDocument("$gt", lastId))
                    )
                )
            );
        }

        return filter.isEmpty() ? after : new BsonDocument("$and", new BsonArray(List.of(filter, after)));
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
