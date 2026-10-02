package io.kestra.plugin.mongodb;

import java.lang.reflect.Field;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Kills a real in-flight {@link Trigger#evaluate()} against MongoDB, on both store paths.
 *
 * <p>
 * Unlike {@link TriggerKillTest}, this test never injects the client/cursor: it runs the real evaluation
 * over a large dedicated collection, applies {@link Trigger#kill()} while fetching is in flight, and
 * verifies the evaluation aborts without publishing output and releases both references.
 */
@KestraTest
public class TriggerEvaluateKillTest extends MongoDbContainer {
    private static final int DOCUMENTS = 30_000;

    @BeforeEach
    void setUp() {
        try (MongoClient client = MongoClients.create(connectionUri)) {
            MongoDatabase database = client.getDatabase("samples");
            MongoCollection<Document> collection = database.getCollection("kill_eval");

            collection.drop();

            List<Document> batch = new ArrayList<>();
            for (int i = 0; i < DOCUMENTS; i++) {
                batch.add(
                    new Document("_id", i)
                        .append("idx", i)
                        .append("payload", "x".repeat(200))
                );
                if (batch.size() == 10_000) {
                    collection.insertMany(batch);
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                collection.insertMany(batch);
            }
        }
    }

    @Test
    void killDuringFetchAbortsEvaluationAndCleansUp() throws Exception {
        killDuringFetchAndVerifyCleanup(false);
    }

    @Test
    void killDuringStoreFetchAbortsEvaluationAndCleansUp() throws Exception {
        killDuringFetchAndVerifyCleanup(true);
    }

    /**
     * Runs a real evaluation over {@value #DOCUMENTS} documents — hundreds of getMore round trips at
     * default driver batching, since the plugin exposes no batch-size knob — and keeps killing until
     * the evaluation thread terminates.
     *
     * <p>
     * This structure cannot fail merely because one kill lands late: kill pressure continues for the
     * whole fetch, and the {@code killFired} gate fails the test if no kill ever observed published
     * handles (for example on infrastructure failure), instead of passing vacuously.
     */
    private void killDuringFetchAndVerifyCleanup(boolean store) throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        Trigger trigger = Trigger.builder()
            .id("watch-kill")
            .connection(
                MongoDbConnection.builder()
                    .uri(Property.ofValue(connectionUri))
                    .build()
            )
            .database(Property.ofValue("samples"))
            .collection(Property.ofValue("kill_eval"))
            .store(Property.ofValue(store))
            .build();

        Flow flow = Flow.builder()
            .id("kill-test")
            .namespace("io.kestra.tests")
            .build();
        ConditionContext conditionContext = ConditionContext.builder()
            .flow(flow)
            .runContext(runContext)
            .build();
        TriggerContext context = TriggerContext.builder()
            .namespace("io.kestra.tests")
            .flowId("kill-test")
            .triggerId("watch-kill")
            .date(ZonedDateTime.now())
            .build();

        AtomicReference<Optional<Execution>> result = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        Thread eval = new Thread(() ->
        {
            try {
                result.set(trigger.evaluate(conditionContext, context));
            } catch (Throwable t) {
                error.set(t);
            } finally {
                done.countDown();
            }
        });
        eval.setDaemon(true);
        eval.start();

        Field cursorField = Trigger.class.getDeclaredField("cursor");
        cursorField.setAccessible(true);
        Field clientField = Trigger.class.getDeclaredField("client");
        clientField.setAccessible(true);

        // Keep kill pressure on until the evaluation thread terminates, so kill lands mid-fetch.
        // Both handles must be observed first: this proves the client publication as well as the
        // cursor publication, and a removed publication fails this test instead of passing silently.
        AtomicBoolean killFired = new AtomicBoolean();
        long deadline = System.currentTimeMillis() + 60_000;
        while (eval.isAlive() && System.currentTimeMillis() < deadline) {
            if (cursorField.get(trigger) != null && clientField.get(trigger) != null) {
                trigger.kill();
                killFired.set(true);
            }
            Thread.sleep(5);
        }

        assertThat("evaluation thread must terminate", done.await(60, TimeUnit.SECONDS), is(true));
        assertThat("kill must land while fetching is in flight", killFired.get(), is(true));
        assertThat("killed evaluation must throw instead of publishing output", error.get(), notNullValue());
        assertThat("no execution may be published from a killed evaluation", result.get(), nullValue());
        assertThat(cursorField.get(trigger), nullValue());
        assertThat(clientField.get(trigger), nullValue());
    }
}
