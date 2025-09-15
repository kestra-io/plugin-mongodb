package io.kestra.plugin.mongodb;

import io.kestra.core.junit.annotations.ExecuteFlow;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.State;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Runner test for MongoDB Aggregate task.
 * This test validates that the Aggregate task configuration can be parsed
 * and executed successfully as part of a Kestra flow.
 */
@KestraTest(startRunner = true)
class RunnerTest extends MongoDbContainer {
    
    /**
     * Test the simple aggregation pipeline flow.
     * This test verifies that the Aggregate task can be properly executed
     * with a basic aggregation pipeline.
     */
    @Test
    @ExecuteFlow("flows/sanity-checks/aggregate-simple.yml")
    void aggregateSimple(Execution execution) {
        assertThat(execution.getTaskRunList(), hasSize(1));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }
    
    /**
     * Test the comprehensive MongoDB aggregation example flow.
     * This test validates that complex aggregation pipelines with multiple stages,
     * lookups, transformations, and various configuration options can be executed.
     * 
     * The flow includes:
     * - Simple grouping and sorting operations
     * - Complex lookups with multiple collections
     * - Field transformations and conditional logic
     * - Time-based aggregations with dynamic parameters
     * - Various configuration options (allowDiskUse, maxTimeMs, store, batchSize)
     */
    @Test
    @ExecuteFlow("flows/mongo-aggregate-example.yml")
    void mongoAggregateExample(Execution execution) {
        assertThat(execution.getTaskRunList(), hasSize(3));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }
}