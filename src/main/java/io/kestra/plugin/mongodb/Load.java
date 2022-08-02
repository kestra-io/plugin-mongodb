package io.kestra.plugin.mongodb;

import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.io.BufferedReader;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Bulk load documents in MongoDB using Kestra Internal Storage file"
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
public class Load extends AbstractLoad {
    @Schema(
        title = "Use this key as id."
    )
    @PluginProperty(dynamic = true)
    private String idKey;

    @Schema(
        title = "Remove idKey from the final document"
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private Boolean removeIdKey = true;

    @SuppressWarnings("unchecked")
    @Override
    protected Flowable<WriteModel<Bson>> source(RunContext runContext, BufferedReader inputStream) {
        return Flowable
            .create(FileSerde.reader(inputStream), BackpressureStrategy.BUFFER)
            .map(o -> {
                Map<String, Object> values = (Map<String, Object>) o;

                if (this.idKey != null) {
                    String idKey = runContext.render(this.idKey);

                    values.put(
                        "_id",
                        new BsonObjectId(new ObjectId(values.get(idKey).toString()))
                    );

                    if (this.removeIdKey) {
                        values.remove(idKey);
                    }
                }

                return new InsertOneModel<>(
                    BsonDocument.parse(JacksonMapper.ofJson().writeValueAsString(values))
                );
            });
    }
}
