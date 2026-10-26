package io.kestra.plugin.mongodb;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;
import jakarta.validation.Validator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * No MongoDB container: the chunk checks live before any client or storage use, so these run
 * without Docker. The container tests in LoadTest and BulkTest still cover real loads.
 */
@KestraTest
class ChunkValidationTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private Validator validator;

    private static Load task(int chunk) {
        return Load.builder()
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

    private static boolean hasChunkViolation(Load task) {
        return validator.validate(task).stream()
            .anyMatch(violation -> violation.getPropertyPath().toString().contains("chunk"));
    }

    @Test
    void chunkZeroFailsValidation() {
        assertThat(hasChunkViolation(task(0)), is(true));
    }

    @Test
    void chunkNegativeFailsValidation() {
        assertThat(hasChunkViolation(task(-5)), is(true));
    }

    @Test
    void chunkOnePassesValidation() {
        assertThat(hasChunkViolation(task(1)), is(false));
    }

    @Test
    void chunkZeroFailsFastWithClearMessage() throws Exception {
        var error = assertThrows(
            IllegalArgumentException.class,
            () -> task(0).run(runContextFactory.of())
        );
        assertThat(error.getMessage(), containsString("chunk"));
    }

    @Test
    void chunkNegativeFailsFastWithClearMessage() throws Exception {
        var error = assertThrows(
            IllegalArgumentException.class,
            () -> task(-5).run(runContextFactory.of())
        );
        assertThat(error.getMessage(), containsString("chunk"));
    }
}
