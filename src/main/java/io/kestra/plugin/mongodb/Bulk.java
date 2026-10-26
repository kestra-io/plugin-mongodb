package io.kestra.plugin.mongodb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import com.mongodb.client.model.*;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run MongoDB bulkWrite from NDJSON",
    description = "Reads newline-delimited bulk operations from internal storage and executes MongoDB bulkWrite. Supports insertOne, updateOne, updateMany, replaceOne, deleteOne and deleteMany in the db.collection.bulkWrite() format (https://www.mongodb.com/docs/manual/reference/method/db.collection.bulkWrite/). insertOne also accepts the legacy flat form where the body is the document itself; a body whose only field is an object named `document` is read as the bulkWrite form. Blank and whitespace-only lines are skipped. Delete, update and replace operations support `collation` and `hint`. Inherits chunking from AbstractLoad (default 1000 per bulk request). The file must be UTF-8: a line that isn't valid UTF-8 fails the task with its line number. When an operation fails, the task fails with its record number (counting emitted operations, not file lines), and its outputs count the operations applied before it, which are kept. A retry runs every line again, so it is only safe for operations that change nothing more when repeated, such as `replaceOne`, or `updateOne` with `$set`, using `upsert: true`: an `insertOne` is inserted again, or fails with a duplicate key error if it sets `_id`, `$inc` or `$push` updates are applied twice, and a `deleteOne` whose filter matches several documents deletes another one."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: mongodb_bulk
                namespace: company.team

                tasks:
                  - id: make_actions
                    type: io.kestra.plugin.core.storage.Write
                    content: |
                      { "insertOne" : {"document": {"firstName": "John", "lastName": "Doe", "city": "Paris"}}}
                      { "insertOne" : {"document": {"firstName": "Ravi", "lastName": "Singh", "city": "Mumbai"}}}
                      { "deleteMany": {"filter": {"city": "Bengaluru"}}}

                  - id: bulk
                    type: io.kestra.plugin.mongodb.Bulk
                    connection:
                      uri: "mongodb://YOUR_HOST:27017"
                    database: "my_database"
                    collection: "my_collection"
                    from: "{{ outputs.make_actions.uri }}"
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
public class Bulk extends AbstractLoad {
    private static final Set<String> DELETE_KEYS = Set.of("filter", "collation", "hint");
    private static final Set<String> REPLACE_KEYS = Set.of("filter", "replacement", "upsert", "bypassDocumentValidation", "collation", "hint");
    private static final Set<String> UPDATE_KEYS = Set.of("filter", "update", "upsert", "collation", "hint", "arrayFilters");

    @Override
    protected Flux<WriteModel<Bson>> source(RunContext runContext, InputStream inputStream) throws IOException {
        return Flux
            .create(this.ndJSonReader(utf8Lines(inputStream)), FluxSink.OverflowStrategy.BUFFER);
    }

    // each line is read as raw bytes (ISO-8859-1 maps every byte to one char) and then decoded as UTF-8 on its own, so invalid
    // UTF-8 fails with its line number instead of becoming U+FFFD; a UTF-8 character never contains the bytes of \n or \r
    private static BufferedReader utf8Lines(InputStream inputStream) {
        CharsetDecoder utf8 = StandardCharsets.UTF_8.newDecoder();

        return new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.ISO_8859_1)) {
            private long line;

            @Override
            public String readLine() throws IOException {
                String bytes = super.readLine();
                if (bytes == null) {
                    return null;
                }

                line++;
                try {
                    return utf8.decode(ByteBuffer.wrap(bytes.getBytes(StandardCharsets.ISO_8859_1))).toString();
                } catch (CharacterCodingException e) {
                    throw new IllegalArgumentException("Line " + line + " is not valid UTF-8", e);
                }
            }
        };
    }

    public Consumer<FluxSink<WriteModel<Bson>>> ndJSonReader(BufferedReader input) throws IOException {
        return throwConsumer(s ->
        {
            String row;
            var lineNumber = 0L;

            // stop reading once downstream cancelled, after a failed bulkWrite or a killed task
            while (!s.isCancelled() && (row = input.readLine()) != null) {
                lineNumber++;

                if (row.isBlank()) {
                    continue;
                }

                try {
                    var bsonDocument = BsonDocument.parse(row);

                    if (bsonDocument.size() != 1) {
                        throw new IllegalArgumentException(
                            "expected exactly 1 operation key, found "
                                + bsonDocument.size() + " on '" + row + "'"
                        );
                    }

                    var operation = bsonDocument.entrySet().iterator().next();
                    if (!operation.getValue().isDocument()) {
                        throw new IllegalArgumentException(operation.getKey() + " requires a document body");
                    }
                    var body = operation.getValue().asDocument();

                    WriteModel<Bson> docWriteRequest = switch (operation.getKey()) {
                        case "insertOne" -> new InsertOneModel<>(
                            MongoDbService.withId(getInsertOneDocument(body))
                        );
                        case "replaceOne" -> {
                            validateKeys("replaceOne", body, REPLACE_KEYS);
                            yield new ReplaceOneModel<>(
                                requireDocument("replaceOne", body, "filter"),
                                requireDocument("replaceOne", body, "replacement"),
                                getReplaceOptions(body)
                            );
                        }
                        case "updateOne" -> {
                            validateKeys("updateOne", body, UPDATE_KEYS);
                            yield new UpdateOneModel<>(
                                requireDocument("updateOne", body, "filter"),
                                requireDocument("updateOne", body, "update"),
                                getUpdateOptions(body)
                            );
                        }
                        case "updateMany" -> {
                            validateKeys("updateMany", body, UPDATE_KEYS);
                            yield new UpdateManyModel<>(
                                requireDocument("updateMany", body, "filter"),
                                requireDocument("updateMany", body, "update"),
                                getUpdateOptions(body)
                            );
                        }
                        case "deleteOne" -> {
                            validateKeys("deleteOne", body, DELETE_KEYS);
                            yield new DeleteOneModel<>(
                                requireDocument("deleteOne", body, "filter"),
                                getDeleteOptions(body)
                            );
                        }
                        case "deleteMany" -> {
                            validateKeys("deleteMany", body, DELETE_KEYS);
                            yield new DeleteManyModel<>(
                                requireDocument("deleteMany", body, "filter"),
                                getDeleteOptions(body)
                            );
                        }
                        default ->
                            throw new IllegalArgumentException("Invalid bulk request type on '" + row + "'");
                    };

                    s.next(docWriteRequest);
                } catch (RuntimeException e) {
                    throw new IllegalArgumentException(
                        "Invalid bulk request on line " + lineNumber + ": " + e.getMessage(), e
                    );
                }
            }

            s.complete();
        });
    }

    private static void validateKeys(String op, BsonDocument body, Set<String> allowed) {
        for (var key : body.keySet()) {
            if (!allowed.contains(key)) {
                throw new IllegalArgumentException(op + " does not support key '" + key + "'");
            }
        }
    }

    private static BsonDocument requireDocument(String op, BsonDocument body, String field) {
        if (!body.containsKey(field) || !body.get(field).isDocument()) {
            String article = field.equals("update") ? "an" : "a";
            throw new IllegalArgumentException(op + " requires " + article + " '" + field + "' document");
        }
        return body.getDocument(field);
    }

    // unwrap the bulkWrite { document: ... } envelope; any other body stays as-is for the legacy flat syntax
    private BsonDocument getInsertOneDocument(BsonDocument insertOne) {
        if (insertOne.size() == 1 && insertOne.isDocument("document")) {
            return insertOne.getDocument("document");
        }

        return insertOne;
    }

    private ReplaceOptions getReplaceOptions(BsonDocument document) {
        var options = new ReplaceOptions();

        if (document.containsKey("upsert") && document.get("upsert").isBoolean()) {
            options.upsert(document.get("upsert").asBoolean().getValue());
        }

        if (document.containsKey("bypassDocumentValidation") && document.get("bypassDocumentValidation").isBoolean()) {
            options.bypassDocumentValidation(document.get("bypassDocumentValidation").asBoolean().getValue());
        }

        if (document.containsKey("collation") && document.get("collation").isDocument()) {
            options.collation(getCollation(document.get("collation").asDocument()));
        }

        applyHint(document, options::hintString, options::hint);

        return options;
    }

    private UpdateOptions getUpdateOptions(BsonDocument document) {
        var options = new UpdateOptions();

        if (document.containsKey("upsert") && document.get("upsert").isBoolean()) {
            options.upsert(document.get("upsert").asBoolean().getValue());
        }

        if (document.containsKey("bypassDocumentValidation") && document.get("bypassDocumentValidation").isBoolean()) {
            options.bypassDocumentValidation(document.get("bypassDocumentValidation").asBoolean().getValue());
        }

        if (document.containsKey("collation") && document.get("collation").isDocument()) {
            options.collation(getCollation(document.get("collation").asDocument()));
        }

        applyHint(document, options::hintString, options::hint);

        if (document.containsKey("arrayFilters") && document.get("arrayFilters").isArray()) {
            var arrayFilters = new ArrayList<Bson>();

            for (var filter : document.get("arrayFilters").asArray()) {
                arrayFilters.add(filter.asDocument());
            }

            options.arrayFilters(arrayFilters);
        }

        return options;
    }

    private DeleteOptions getDeleteOptions(BsonDocument document) {
        var options = new DeleteOptions();

        if (document.containsKey("collation") && document.get("collation").isDocument()) {
            options.collation(getCollation(document.get("collation").asDocument()));
        }

        applyHint(document, options::hintString, options::hint);

        return options;
    }

    private static void applyHint(BsonDocument document, Consumer<String> hintString, Consumer<Bson> hint) {
        if (document.containsKey("hint")) {
            var value = document.get("hint");
            if (value.isString()) {
                hintString.accept(value.asString().getValue());
            } else if (value.isDocument()) {
                hint.accept(value.asDocument());
            } else {
                throw new IllegalArgumentException("hint must be an index name or a key pattern document");
            }
        }
    }

    private Collation getCollation(BsonDocument document) {
        Collation.Builder builder = Collation.builder();

        Map<String, Consumer<BsonValue>> collationOptions = Map.of(
            "locale", value -> builder.locale(value.asString().getValue()),
            "caseLevel", value -> builder.caseLevel(value.asBoolean().getValue()),
            "caseFirst", value -> builder.collationCaseFirst(CollationCaseFirst.fromString(value.asString().getValue())),
            "strength", value -> builder.collationStrength(CollationStrength.fromInt(value.asInt32().getValue())),
            "numericOrdering", value -> builder.numericOrdering(value.asBoolean().getValue()),
            "alternate", value -> builder.collationAlternate(CollationAlternate.fromString(value.asString().getValue())),
            "maxVariable", value -> builder.collationMaxVariable(CollationMaxVariable.fromString(value.asString().getValue())),
            "normalization", value -> builder.normalization(value.asBoolean().getValue()),
            "backwards", value -> builder.backwards(value.asBoolean().getValue())
        );

        document.forEach((key, value) ->
        {
            if (collationOptions.containsKey(key)) {
                collationOptions.get(key).accept(value);
            }
        });

        return builder.build();
    }

}
