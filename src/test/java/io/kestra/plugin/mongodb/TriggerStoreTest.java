package io.kestra.plugin.mongodb;

import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.EvaluateTrigger;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Exercises the polling {@link Trigger} with {@code store=true}.
 *
 * <p>
 * This covers the Trigger-side cursor iteration used by the {@code store} path
 * (explicit {@code MongoCursor} retained for {@link Trigger#kill()}, streamed
 * through {@code FileSerde} into internal storage). It reads the same shared
 * {@code samples.books} dataset as {@link TriggerTest}, so it needs the same
 * CI infrastructure (MongoDB on {@code localhost:27017} seeded by
 * {@code setup-unit.sh}).
 */
@KestraTest(startRunner = true, startScheduler = true)
public class TriggerStoreTest extends MongoDbContainer {
    @Test
    @EvaluateTrigger(flow = "flows/mongo-listen-store.yml", triggerId = "watch-store")
    void run(Optional<Execution> optionalExecution) {
        assertThat(optionalExecution.isPresent(), is(true));
        Execution execution = optionalExecution.get();

        Map<String, Object> variables = execution.getTrigger().getVariables();

        assertThat(((Number) variables.get("size")).longValue(), is(265L));
        assertThat(variables.get("uri"), notNullValue());
        assertThat(variables.get("rows"), nullValue());
    }
}
