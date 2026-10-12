package io.kestra.plugin.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@NoArgsConstructor
@Getter
public class MongoDbConnection {
    @Schema(
        title = "Connection string to MongoDB server",
        description = "[URL format](https://docs.mongodb.com/manual/reference/connection-string/) " +
            "like `mongodb://mongodb0.example.com:27017`"
    )
    @NotNull
    @PluginProperty(secret = true)
    private Property<@NotEmpty String> uri;

    MongoClientSettings settings(RunContext runContext) throws IllegalVariableEvaluationException {
        return MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString(runContext.render(uri).as(String.class).orElseThrow()))
            .build();
    }
}
