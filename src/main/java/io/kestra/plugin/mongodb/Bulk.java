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
            code = {
                "connection:",
                "  uri: \"mongodb://root:example@localhost:27017/?authSource=admin\"",
                "database: \"my_database\"",
                "collection: \"my_collection\"",
                "from: \"{{ inputs.file }}\"",
            }
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

                WriteModel<Bson> docWriteRequest;

                switch (operation.getKey()) {
                    case "insertOne":
                        docWriteRequest = new InsertOneModel<>(
                            operation.getValue().asDocument()
                        );
                        break;
                    case "replaceOne":
                        docWriteRequest = new ReplaceOneModel<>(
                            operation.getValue().asDocument().get("filter").asDocument(),
                            operation.getValue().asDocument().get("replacement").asDocument()
                        );
                        break;
                    case "updateOne":
                        docWriteRequest = new UpdateOneModel<>(
                            operation.getValue().asDocument().get("filter").asDocument(),
                            operation.getValue().asDocument().get("update").asDocument()
                        );
                        break;
                    case "updateMany":
                        docWriteRequest = new UpdateManyModel<>(
                            operation.getValue().asDocument().get("filter").asDocument(),
                            operation.getValue().asDocument().get("update").asDocument()
                        );
                        break;
                    case "deleteOne":
                        docWriteRequest = new DeleteOneModel<>(
                            operation.getValue().asDocument().get("filter").asDocument()
                        );
                        break;
                    case "deleteMany":
                        docWriteRequest = new DeleteManyModel<>(
                            operation.getValue().asDocument().get("filter").asDocument()
                        );
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid bulk request type on '" + row + "'");
                }

                s.next(docWriteRequest);
            }

            s.complete();
        });
    }
}
