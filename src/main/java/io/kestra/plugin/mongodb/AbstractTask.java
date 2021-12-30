package io.kestra.plugin.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractTask extends Task {
    @Schema(
        title = "The connection properties."
    )
    @NotNull
    protected MongoDbConnection connection;

    @Schema(
        title = "The mongodb database."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected String database;

    @Schema(
        title = "The mongodb collection."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected String collection;

    protected MongoCollection<Bson> collection(RunContext runContext, MongoClient client) throws IllegalVariableEvaluationException {
        return this.collection(runContext, client, Bson.class);
    }

    protected <T> MongoCollection<T> collection(RunContext runContext, MongoClient client, Class<T> cls) throws IllegalVariableEvaluationException {
        MongoDatabase database = client.getDatabase(runContext.render(this.database));
        return database.getCollection(
            runContext.render(this.collection),
            cls
        );
    }
}
