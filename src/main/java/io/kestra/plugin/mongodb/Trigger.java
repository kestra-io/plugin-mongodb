package io.kestra.plugin.mongodb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Poll MongoDB and trigger on results",
    description = "Periodically runs a MongoDB find; if results are non-empty, starts a Flow with the rows or stored file. Uses Find task behavior (filter/projection/sort/limit/skip). Default interval is 60s and store is false, returning rows in trigger output."
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for a MongoDB query to return results, and then iterate through returned documents.",
            full = true,
            code = """
                id: mongodb_trigger
                namespace: company.team

                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.rows }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ json(taskrun.value) }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.mongodb.Trigger
                    interval: "PT5M"
                    connection:
                      uri: mongodb://root:example@localhost:27017/?authSource=admin
                    database: samples
                    collection: books
                    filter:
                      pageCount:
                        $gte: 50
                    sort:
                      pageCount: -1
                    projection:
                      title: 1
                      publishedDate: 1
                      pageCount: 1
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Find.Output> {

    @Schema(
        title = "Polling interval",
        description = "Duration between queries; defaults to PT60S."
    )
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    private MongoDbConnection connection;

    @Schema(
        title = "Database name"
    )
    private Property<String> database;

    @Schema(
        title = "Collection name"
    )
    private Property<String> collection;

    @Schema(
        title = "Query filter",
        description = "BSON string or map rendered before execution."
    )
    private Object filter;

    @Schema(
        title = "Projection",
        description = "BSON string or map selecting fields to return."
    )
    private Object projection;

    @Schema(
        title = "Sort",
        description = "BSON string or map defining sort order."
    )
    private Object sort;

    @Schema(
        title = "Limit",
        description = "Maximum documents returned."
    )
    private Property<Integer> limit;

    @Schema(
        title = "Skip",
        description = "Documents to skip before returning results."
    )
    private Property<Integer> skip;

    @Schema(
        title = "Store results",
        description = "When true, writes results as Ion to internal storage; otherwise rows are kept in output. Defaults to false."
    )
    @Builder.Default
    private Property<Boolean> store = Property.ofValue(false);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        Find find = Find.builder()
            .id(this.id)
            .type(Find.class.getName())
            .connection(this.connection)
            .database(this.database)
            .collection(this.collection)
            .filter(this.filter)
            .projection(this.projection)
            .sort(this.sort)
            .limit(this.limit)
            .skip(this.skip)
            .store(this.store)
            .build();

        Find.Output output = find.run(runContext);

        logger.debug("Found '{}' rows", output.getSize());

        if(Optional.ofNullable(output.getSize()).orElse(0L) == 0) {
            return Optional.empty();
        }

        return Optional.of(
            TriggerService.generateExecution(this, conditionContext, context, output)
        );
    }

}
