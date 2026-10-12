package io.kestra.plugin.mongodb;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoClient;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.flows.GenericFlow;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.repositories.FlowRepositoryInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.TestRunnerUtils;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Kills tasks and a trigger while MongoDB is running their operation, and checks that the server stops it.
 *
 * <p>
 * Every operation filters with {@code $where: sleep(100)}, so the server spends about 100 ms per document. Closing
 * the client does not stop such an operation: without killing its session, the killed call only returns, and a write
 * only stops, once the server has gone through every document.
 */
@KestraTest(startRunner = true)
class KillTest extends MongoDbContainer {
    private static final String COLLECTION = "docs";
    private static final String SLOW_FILTER = "{\"$where\": \"sleep(100) || true\"}";
    // far below the ~10 s an operation over 100 documents takes when it is not killed
    private static final long KILLED_WITHIN_SECONDS = 5;

    private static MongoClient monitor;

    @Inject
    private FlowRepositoryInterface flowRepository;

    @Inject
    private TestRunnerUtils runnerUtils;

    @BeforeAll
    static void openMonitor() {
        monitor = getMongoClient();
    }

    @AfterAll
    static void closeMonitor() {
        monitor.close();
    }

    @Test
    void killBeforeRunDoesNothing() {
        Find find = Find.builder()
            .id("find")
            .type(Find.class.getName())
            .connection(connection())
            .build();

        find.kill();
        find.kill();
    }

    @Test
    void killStopsFindOnTheServer() throws Exception {
        String database = seed(100);
        Find find = Find.builder()
            .id("find")
            .type(Find.class.getName())
            .connection(connection())
            .database(Property.ofValue(database))
            .collection(Property.ofValue(COLLECTION))
            .filter(SLOW_FILTER)
            .build();

        FutureTask<Find.Output> run = start(() -> find.run(runContextFactory.of()));
        awaitOperation(database);

        find.kill();

        assertThrows(ExecutionException.class, () -> run.get(KILLED_WITHIN_SECONDS, TimeUnit.SECONDS));
        awaitNoOperation(database);
    }

    @Test
    void killStopsUpdateManyWrites() throws Exception {
        String database = seed(100);
        Update update = Update.builder()
            .id("update")
            .type(Update.class.getName())
            .connection(connection())
            .database(Property.ofValue(database))
            .collection(Property.ofValue(COLLECTION))
            .operation(Property.ofValue(Update.Operation.UPDATE_MANY))
            .filter(SLOW_FILTER)
            .document("{\"$set\": {\"touched\": true}}")
            .build();

        FutureTask<Update.Output> run = start(() -> update.run(runContextFactory.of()));
        awaitOperation(database);

        update.kill();

        assertThrows(ExecutionException.class, () -> run.get(KILLED_WITHIN_SECONDS, TimeUnit.SECONDS));
        awaitNoOperation(database);
        assertThat(
            "the server must stop updating documents once killed",
            monitor.getDatabase(database).getCollection(COLLECTION).countDocuments(new Document("touched", true)),
            lessThan(100L)
        );
    }

    @Test
    void killStopsTriggerEvaluationOnTheServer() throws Exception {
        String database = seed(100);
        Trigger trigger = Trigger.builder()
            .id("watch")
            .type(Trigger.class.getName())
            .connection(connection())
            .database(Property.ofValue(database))
            .collection(Property.ofValue(COLLECTION))
            .filter(SLOW_FILTER)
            .build();
        RunContext runContext = runContextFactory.of(Map.of());
        ConditionContext conditionContext = ConditionContext.builder()
            .flow(Flow.builder().id("kill").namespace("io.kestra.tests").build())
            .runContext(runContext)
            .build();
        TriggerContext triggerContext = TriggerContext.builder()
            .namespace("io.kestra.tests")
            .flowId("kill")
            .triggerId("watch")
            .date(ZonedDateTime.now())
            .build();

        FutureTask<Optional<Execution>> run = start(() -> trigger.evaluate(conditionContext, triggerContext));
        awaitOperation(database);

        trigger.kill();

        assertThrows(ExecutionException.class, () -> run.get(KILLED_WITHIN_SECONDS, TimeUnit.SECONDS));
        awaitNoOperation(database);
    }

    @Test
    void killingTheExecutionStopsTheOperationOnTheServer() throws Exception {
        String database = seed(100);
        FlowWithSource flow = flowRepository.create(GenericFlow.fromYaml(TenantService.MAIN_TENANT, """
            id: kill-%s
            namespace: io.kestra.tests

            tasks:
              - id: find
                type: io.kestra.plugin.mongodb.Find
                connection:
                  uri: "%s"
                database: "%s"
                collection: "%s"
                filter:
                  $where: "sleep(100) || true"
            """.formatted(IdUtils.create().toLowerCase(Locale.ROOT), connectionUri, database, COLLECTION)));
        Execution execution = Execution.newExecution(flow, List.of());

        start(() -> runnerUtils.emitAndAwaitExecution(e -> e.getState().isTerminated(), execution));
        awaitOperation(database);

        // through the worker, which calls kill() then interrupts the task thread
        FutureTask<Execution> kill = start(() -> runnerUtils.killExecution(execution));

        awaitNoOperation(database);
        assertThat(kill.get(30, TimeUnit.SECONDS).getState().getCurrent(), is(State.Type.KILLED));
    }

    @Test
    void killOnlyStopsTheKilledClient() throws Exception {
        String database = seed(100);
        String otherDatabase = seed(30);
        Find find = Find.builder()
            .id("find")
            .type(Find.class.getName())
            .connection(connection())
            .database(Property.ofValue(database))
            .collection(Property.ofValue(COLLECTION))
            .filter(SLOW_FILTER)
            .build();

        // same user, another client: its sessions must survive the kill
        FutureTask<List<Document>> other = start(() -> slowFind(otherDatabase));
        FutureTask<Find.Output> run = start(() -> find.run(runContextFactory.of()));
        awaitOperation(database);
        awaitOperation(otherDatabase);

        find.kill();

        assertThrows(ExecutionException.class, () -> run.get(KILLED_WITHIN_SECONDS, TimeUnit.SECONDS));
        assertThat(other.get(30, TimeUnit.SECONDS), hasSize(30));
    }

    @Test
    void killSessionsWithoutRecordedSessionsKillsNothing() throws Exception {
        String database = seed(30);
        FutureTask<List<Document>> other = start(() -> slowFind(database));
        awaitOperation(database);

        // killSessions with an empty array would kill every session of the user, including the one running above
        MongoDbSessionKiller sessionKiller = new MongoDbSessionKiller(connection().settings(runContextFactory.of()));
        try (MongoClient unused = sessionKiller.client()) {
            sessionKiller.killSessions();
        }

        assertThat(other.get(30, TimeUnit.SECONDS), hasSize(30));
    }

    private static MongoDbConnection connection() {
        return MongoDbConnection.builder()
            .uri(Property.ofValue(connectionUri))
            .build();
    }

    private static String seed(int documents) {
        String database = "kill_" + IdUtils.create().toLowerCase(Locale.ROOT);
        List<Document> batch = new ArrayList<>();
        for (int i = 0; i < documents; i++) {
            batch.add(new Document("_id", i).append("touched", false));
        }
        monitor.getDatabase(database).getCollection(COLLECTION).insertMany(batch);
        return database;
    }

    private static List<Document> slowFind(String database) {
        try (MongoClient client = getMongoClient()) {
            return client.getDatabase(database)
                .getCollection(COLLECTION)
                .find(Document.parse(SLOW_FILTER))
                .into(new ArrayList<>());
        }
    }

    private static <T> FutureTask<T> start(Callable<T> callable) {
        FutureTask<T> task = new FutureTask<>(callable);
        Thread thread = new Thread(task);
        thread.setDaemon(true);
        thread.start();
        return task;
    }

    private static List<Document> operations(String database) {
        return monitor.getDatabase("admin")
            .aggregate(
                List.of(
                    new Document("$currentOp", new Document("allUsers", true)),
                    new Document("$match", new Document("ns", database + "." + COLLECTION).append("active", true))
                )
            )
            .into(new ArrayList<>());
    }

    private static void awaitOperation(String database) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (operations(database).isEmpty()) {
            assertThat("the operation must reach the server", System.currentTimeMillis() < deadline, is(true));
            Thread.sleep(20);
        }
    }

    private static void awaitNoOperation(String database) throws InterruptedException {
        // an operation that is not killed keeps running for several more seconds
        long deadline = System.currentTimeMillis() + 4_000;
        while (!operations(database).isEmpty()) {
            assertThat("the server must stop the killed operation", System.currentTimeMillis() < deadline, is(true));
            Thread.sleep(20);
        }
    }
}
