package io.kestra.plugin.mongodb;

import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
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
    title = "Bulk load documents into a MongoDB using Kestra’s internal storage file."
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
    }
)
public class Load extends AbstractLoad {
    @Schema(
        title = "Use this key as ID"
    )
    private Property<String> idKey;

    @Schema(
        title = "Whether to remove idKey from the final document"
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
