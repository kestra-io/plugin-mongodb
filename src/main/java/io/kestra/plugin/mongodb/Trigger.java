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
    title = "Trigger a flow if a periodically executed MongoDB query returns a non-empty result set."
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

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    private MongoDbConnection connection;

    private Property<String> database;

    private Property<String> collection;

    private Object filter;

    private Object projection;

    private Object sort;

    private Property<Integer> limit;

    private Property<Integer> skip;

    @Builder.Default
    private Property<Boolean> store = Property.of(false);

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
