package io.kestra.plugin.mongodb;

import com.mongodb.client.model.*;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute [Bulk](https://www.mongodb.com/docs/manual/reference/method/Bulk/) request in MongoDB.",
    description = """
    Here are the sample file contents that can be provided as input to Bulk task:
    ```
    { "insertOne" : {"firstName": "John", "lastName": "Doe", "city": "Paris"}}
    { "insertOne" : {"firstName": "Ravi", "lastName": "Singh", "city": "Mumbai"}}
    { "deleteMany": {"filter": {"city": "Bengaluru"}}}
    ```
    """
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: mongodb_bulk
                namespace: company.team
                
                inputs:
                  - id: myfile
                    type: FILE
                
                tasks:
                  - id: bulk
                    type: io.kestra.plugin.mongodb.Bulk
                    connection:
                      uri: "mongodb://root:example@localhost:27017/?authSource=admin"
                    database: "my_database"
                    collection: "my_collection"
                    from: "{{ inputs.myfile }}"
                """
        )
    }
)
public class Bulk extends AbstractLoad {
    @Override
    protected Flux<WriteModel<Bson>> source(RunContext runContext, BufferedReader inputStream) throws IOException {
        return Flux
            .create(this.ndJSonReader(inputStream), FluxSink.OverflowStrategy.BUFFER);
    }

    public Consumer<FluxSink<WriteModel<Bson>>> ndJSonReader(BufferedReader input) throws IOException {
        return throwConsumer(s -> {
            String row;

            while ((row = input.readLine()) != null) {
                BsonDocument bsonDocument = BsonDocument.parse(row);
                Map.Entry<String, BsonValue> operation = bsonDocument.entrySet().iterator().next();

                WriteModel<Bson> docWriteRequest = switch (operation.getKey()) {
                    case "insertOne" -> new InsertOneModel<>(
                        operation.getValue().asDocument()
                    );
                    case "replaceOne" ->  new ReplaceOneModel<>(
                        operation.getValue().asDocument().get("filter").asDocument(),
                        operation.getValue().asDocument().get("replacement").asDocument(),
                        getReplaceOptions(operation.getValue().asDocument())
                    );
                    case "updateOne" -> new UpdateOneModel<>(
                        operation.getValue().asDocument().get("filter").asDocument(),
                        operation.getValue().asDocument().get("update").asDocument(),
                        getUpdateOptions(operation.getValue().asDocument())
                    );
                    case "updateMany" -> new UpdateManyModel<>(
                        operation.getValue().asDocument().get("filter").asDocument(),
                        operation.getValue().asDocument().get("update").asDocument(),
                        getUpdateOptions(operation.getValue().asDocument())
                    );
                    case "deleteOne" -> new DeleteOneModel<>(
                        operation.getValue().asDocument().get("filter").asDocument()
                    );
                    case "deleteMany" -> new DeleteManyModel<>(
                        operation.getValue().asDocument().get("filter").asDocument()
                    );
                    default ->
                        throw new IllegalArgumentException("Invalid bulk request type on '" + row + "'");
                };

                s.next(docWriteRequest);
            }

            s.complete();
        });
    }

    private ReplaceOptions getReplaceOptions(BsonDocument document) {
        ReplaceOptions options = new ReplaceOptions();

        if (document.containsKey("upsert") && document.get("upsert").isBoolean()) {
            options.upsert(document.get("upsert").asBoolean().getValue());
        }

        if (document.containsKey("bypassDocumentValidation") && document.get("bypassDocumentValidation").isBoolean()) {
            options.bypassDocumentValidation(document.get("bypassDocumentValidation").asBoolean().getValue());
        }

        if (document.containsKey("collation") && document.get("collation").isDocument()) {
            options.collation(getCollation(document.get("collation").asDocument()));
        }

        return options;
    }

    private UpdateOptions getUpdateOptions(BsonDocument document) {
        UpdateOptions options = new UpdateOptions();

        if (document.containsKey("upsert") && document.get("upsert").isBoolean()) {
            options.upsert(document.get("upsert").asBoolean().getValue());
        }

        if (document.containsKey("bypassDocumentValidation") && document.get("bypassDocumentValidation").isBoolean()) {
            options.bypassDocumentValidation(document.get("bypassDocumentValidation").asBoolean().getValue());
        }

        if (document.containsKey("collation") && document.get("collation").isDocument()) {
            options.collation(getCollation(document.get("collation").asDocument()));
        }

        if (document.containsKey("arrayFilters") && document.get("arrayFilters").isArray()) {
            List<Bson> arrayFilters = new ArrayList<>();

            for (BsonValue filter : document.get("arrayFilters").asArray()) {
                arrayFilters.add(filter.asDocument());
            }

            options.arrayFilters(arrayFilters);
        }

        return options;
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

        document.forEach((key, value) -> {
            if (collationOptions.containsKey(key)) {
                collationOptions.get(key).accept(value);
            }
        });

        return builder.build();
    }

}
