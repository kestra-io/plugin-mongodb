package io.kestra.plugin.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@NoArgsConstructor
@Getter
public class MongoDbConnection {
    @Schema(
        title = "Connection string to MongoDB server.",
        description = "[URL format](https://docs.mongodb.com/manual/reference/connection-string/) " +
            "like `mongodb://mongodb0.example.com:27017`"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @NotEmpty
    private String uri;


    MongoClient client(RunContext runContext) throws IllegalVariableEvaluationException {
        return MongoClients.create(runContext.render(uri));
    }
}
