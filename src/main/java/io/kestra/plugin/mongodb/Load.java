package io.kestra.plugin.mongodb;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Bulk insert documents from internal storage",
    description = """
        Reads a Kestra internal storage file of JSON/BSON records and inserts them with MongoDB bulkWrite. Inherits chunking (default 1000). Optionally derives _id from a field and removes that field. Objects whose first key is a MongoDB Extended JSON type wrapper (such as `$date`, `$oid` or `$numberDecimal`) are decoded to that BSON type; a malformed wrapper fails the task with the record and field it was found in. When a write fails, the task fails with the number of the failing record, and its outputs count the records applied before it, which stay in the collection. A retry reads the whole file again: without `idKey`, the records already written are inserted a second time; with `idKey`, records with a present `idKey` fail on retry with a duplicate key error (records where `idKey` is missing or null receive a generated ObjectId and are inserted again)."""
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: mongodb_load
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: load
                    type: io.kestra.plugin.mongodb.Load
                    connection:
                      uri: "mongodb://root:example@localhost:27017/?authSource=admin"
                    database: "my_database"
                    collection: "my_collection"
                    from: "{{ inputs.file }}"
                """
        )
    },
    metrics = {
        @Metric(
            name = "records",
            type = Counter.TYPE,
            unit = "count",
            description = "Number of documents processed in the bulk operation; when a write fails, those applied before it"
        ),
        @Metric(
            name = "requests.count",
            type = Counter.TYPE,
            unit = "count",
            description = "Number of bulk requests sent to MongoDB"
        )
    }
)
public class Load extends AbstractLoad {
    @Schema(
        title = "Field used as _id",
        description = """
            If set, the value of this field is stored as `_id` (converted to an `ObjectId` if a 24-character hex string or an `$oid` wrapper, or stored directly as a custom primary key). If the field is missing or null, a new `ObjectId` is generated."""
    )
    @PluginProperty(group = "connection")
    private Property<String> idKey;

    @Schema(
        title = "Remove idKey field",
        description = "When true (default), drops the source field after copying it to _id. Has no effect when `idKey` is `_id`."
    )
    @Builder.Default
    @PluginProperty(group = "connection")
    private Property<Boolean> removeIdKey = Property.ofValue(true);

    // first keys that the driver's JsonReader (5.12.0) decodes as a BSON type wrapper; any other object stays a document
    private static final Set<String> EXTENDED_JSON_WRAPPER_KEYS = Set.of(
        "$binary", "$code", "$date", "$dbPointer", "$maxKey", "$minKey", "$numberDecimal", "$numberDouble", "$numberInt",
        "$numberLong", "$oid", "$options", "$regex", "$regularExpression", "$symbol", "$timestamp", "$type", "$undefined", "$uuid"
    );

    @SuppressWarnings("unchecked")
    @Override
    protected Flux<WriteModel<Bson>> source(RunContext runContext, InputStream inputStream) throws Exception {
        var recordNumber = new AtomicLong();
        var rIdKey = runContext.render(this.idKey).as(String.class);
        var rRemoveIdKey = runContext.render(this.removeIdKey).as(Boolean.class).orElse(true);
        var missingIdCount = new AtomicLong();

        return FileSerde.readAll(inputStream)
            .<WriteModel<Bson>>map(throwFunction(o ->
            {
                var values = (Map<String, Object>) o;

                // decoded first so that an idKey given as an $oid wrapper is already an ObjectId
                decodeExtendedJsonFields(values, recordNumber.incrementAndGet(), "");

                if (rIdKey.isPresent()) {
                    var idKey = rIdKey.get();
                    var id = values.get(idKey);

                    if (id != null) {
                        var idValue = id instanceof String str && ObjectId.isValid(str) ? new BsonObjectId(new ObjectId(str)) : id;
                        values.put("_id", idValue);
                    } else {
                        missingIdCount.incrementAndGet();
                    }

                    // with idKey _id, the source field is the _id just set, so it is kept
                    if (!idKey.equals("_id") && rRemoveIdKey) {
                        values.remove(idKey);
                    }
                }

                // ordinary maps and values go straight into a Document, only Extended JSON wrappers were decoded above
                return new InsertOneModel<Bson>(new Document(MongoDbService.withId(values)));
            }))
            .doOnComplete(() -> {
                if (missingIdCount.get() > 0 && rIdKey.isPresent()) {
                    runContext.logger().warn(
                        "Found {} record(s) with missing or null '{}', generated an ObjectId for each.",
                        missingIdCount.get(),
                        rIdKey.get()
                    );
                }
            });
    }

    private static void decodeExtendedJsonFields(Map<String, Object> document, long record, String path) throws JsonProcessingException {
        for (var entry : document.entrySet()) {
            entry.setValue(decodeExtendedJson(entry.getValue(), record, path.isEmpty() ? entry.getKey() : path + "." + entry.getKey()));
        }
    }

    @SuppressWarnings("unchecked")
    private static Object decodeExtendedJson(Object value, long record, String path) throws JsonProcessingException {
        if (value instanceof Map<?, ?> map) {
            // the driver's JsonReader only interprets an object as Extended JSON based on its first key
            String firstKey = map.isEmpty() ? null : map.keySet().iterator().next().toString();
            if (firstKey != null && EXTENDED_JSON_WRAPPER_KEYS.contains(firstKey)) {
                BsonValue decoded;
                try {
                    decoded = BsonDocument.parse(JacksonMapper.ofJson().writeValueAsString(Map.of("value", map))).get("value");
                } catch (RuntimeException e) {
                    throw new IllegalArgumentException(
                        "Record " + record + ", field '" + path + "': value looks like an Extended JSON " + firstKey + " wrapper but could not be decoded: " + e.getMessage(),
                        e
                    );
                }
                if (!decoded.isDocument()) {
                    return decoded;
                }
            }
            decodeExtendedJsonFields((Map<String, Object>) map, record, path);
        } else if (value instanceof List<?> list) {
            var iterator = ((List<Object>) list).listIterator();
            while (iterator.hasNext()) {
                int index = iterator.nextIndex();
                iterator.set(decodeExtendedJson(iterator.next(), record, path + "[" + index + "]"));
            }
        }
        return value;
    }
}
