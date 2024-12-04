package io.kestra.plugin.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.bson.conversions.Bson;

import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractTask extends Task {
    @Schema(
        title = "MongoDB connection properties."
    )
    @NotNull
    protected MongoDbConnection connection;

    @Schema(
        title = "MongoDB database."
    )
    @NotNull
    protected Property<String> database;

    @Schema(
        title = "MongoDB collection."
    )
    @NotNull
    protected Property<String> collection;

    protected MongoCollection<Bson> collection(RunContext runContext, MongoClient client) throws IllegalVariableEvaluationException {
        return this.collection(runContext, client, Bson.class);
    }

    protected <T> MongoCollection<T> collection(RunContext runContext, MongoClient client, Class<T> cls) throws IllegalVariableEvaluationException {
        MongoDatabase database = client.getDatabase(runContext.render(this.database).as(String.class).orElseThrow());
        return database.getCollection(
            runContext.render(this.collection).as(String.class).orElseThrow(),
            cls
        );
    }
}
