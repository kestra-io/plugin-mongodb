package io.kestra.plugin.mongodb;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoClient;

import io.kestra.core.junit.annotations.EvaluateTrigger;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Exercises the polling {@link Trigger} with {@code store=true}.
 *
 * <p>
 * This covers the {@code store=true} output contract (result URI and size in
 * internal storage, no in-memory rows) and checks that a poll which finds nothing
 * leaves no file behind. {@link #run} reads the same {@code samples.books}
 * documents as {@link TriggerTest}, seeded in the test container. It is not kill
 * wiring coverage: see {@link TriggerEvaluateKillTest} for kill-during-fetch behavior.
 */
@KestraTest(startRunner = true, startScheduler = true)
public class TriggerStoreTest extends MongoDbContainer {
    @Inject
    private StorageInterface storageInterface;

    @BeforeEach
    void setUp() {
        TriggerTest.seedBooks();
    }

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

    @Test
    void pollThatFindsNothingLeavesNoFileInInternalStorage() throws Exception {
        var collection = collectionWith(new Document("_id", 1).append("status", "done"));
        var trigger = trigger(collection);
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));
        assertThat(triggerFiles(context), empty());
    }

    @Test
    void pollThatFindsDocumentsKeepsItsFile() throws Exception {
        var collection = collectionWith(new Document("_id", 1).append("status", "pending"));
        var trigger = trigger(collection);
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        var execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));
        var variables = execution.get().getTrigger().getVariables();
        assertThat(((Number) variables.get("size")).longValue(), is(1L));
        var uri = URI.create((String) variables.get("uri"));
        assertThat(context.getKey().getRunContext().storage().isFileExist(uri), is(true));
        assertThat(triggerFiles(context), contains(hasToString(endsWith(uri.getPath()))));
    }

    private static String collectionWith(Document document) {
        var collection = "store_" + IdUtils.create();
        try (MongoClient client = getMongoClient()) {
            client.getDatabase("samples").getCollection(collection).insertOne(document);
        }
        return collection;
    }

    private static Trigger trigger(String collection) {
        return Trigger.builder()
            .id("watch_" + IdUtils.create())
            .type(Trigger.class.getName())
            .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue("samples"))
            .collection(Property.ofValue(collection))
            .filter(Map.of("status", "pending"))
            .store(Property.ofValue(true))
            .build();
    }

    // every file under the internal storage directory this trigger evaluation writes to
    private List<URI> triggerFiles(Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context) throws Exception {
        var triggerContext = context.getValue();
        var prefix = context.getKey().getRunContext().storage().getContextBaseURI();
        return storageInterface.allByPrefix(triggerContext.getTenantId(), triggerContext.getNamespace(), prefix, false);
    }
}
