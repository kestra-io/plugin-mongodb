package io.kestra.plugin.mongodb;

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
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import reactor.core.publisher.Flux;

import java.io.BufferedReader;
import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Bulk insert documents from internal storage",
    description = "Reads a Kestra internal storage file of JSON/BSON records and inserts them with MongoDB bulkWrite. Inherits chunking (default 1000). Optionally derives _id from a field and removes that field."
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
            description = "Number of documents processed in the bulk operation"
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
        description = "If set, value is converted to ObjectId and stored as _id."
    )
    private Property<String> idKey;

    @Schema(
        title = "Remove idKey field",
        description = "When true (default), drops the source field after copying it to _id."
    )
    @Builder.Default
    private Property<Boolean> removeIdKey = Property.ofValue(true);

    @SuppressWarnings("unchecked")
    @Override
    protected Flux<WriteModel<Bson>> source(RunContext runContext, BufferedReader inputStream) throws Exception {
        return FileSerde.readAll(inputStream)
            .map(throwFunction(o -> {
                Map<String, Object> values = (Map<String, Object>) o;

                if (runContext.render(this.idKey).as(String.class).isPresent()) {
                    String idKey = runContext.render(this.idKey).as(String.class).get();

                    values.put(
                        "_id",
                        new BsonObjectId(new ObjectId(values.get(idKey).toString()))
                    );

                    if (runContext.render(this.removeIdKey).as(Boolean.class).orElseThrow()) {
                        values.remove(idKey);
                    }
                }

                return new InsertOneModel<>(
                    BsonDocument.parse(JacksonMapper.ofJson().writeValueAsString(values))
                );
            }));
    }
}
