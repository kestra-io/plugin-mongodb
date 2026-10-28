package io.kestra.plugin.mongodb;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** No container: the chunk checks run before any client or storage use. */
@KestraTest
class ChunkValidationTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private Validator validator;

    private static Load task(int chunk) {
        return Load.builder()
            .id(IdUtils.create())
            .type(Load.class.getName())
            .connection(
                MongoDbConnection.builder()
                    .uri(Property.ofValue("mongodb://localhost:27017"))
                    .build()
            )
            .database(Property.ofValue("test"))
            .collection(Property.ofValue("docs"))
            .from(Property.ofValue("kestra://test"))
            .chunk(Property.ofValue(chunk))
            .build();
    }

    private boolean hasChunkViolation(Load task) {
        return validator.validate(task).stream()
            .anyMatch(violation -> violation.getPropertyPath().toString().contains("chunk"));
    }

    @Test
    void chunkZeroViolatesMinConstraint() {
        assertThat(hasChunkViolation(task(0)), is(true));
    }

    @Test
    void chunkNegativeViolatesMinConstraint() {
        assertThat(hasChunkViolation(task(-5)), is(true));
    }

    @Test
    void chunkOneSatisfiesMinConstraint() {
        assertThat(hasChunkViolation(task(1)), is(false));
    }

    @Test
    void chunkZeroRejectedOnWorkerWithClearMessage() throws Exception {
        var task = task(0);
        var error = assertThrows(
            ConstraintViolationException.class,
            () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()))
        );
        assertThat(error.getMessage(), containsString("chunk"));
    }

    @Test
    void chunkNegativeRejectedOnWorkerWithClearMessage() throws Exception {
        var task = task(-5);
        var error = assertThrows(
            ConstraintViolationException.class,
            () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()))
        );
        assertThat(error.getMessage(), containsString("chunk"));
    }
}
