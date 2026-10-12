package io.kestra.plugin.mongodb;

import java.util.concurrent.atomic.AtomicReference;

import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.WorkerJobLifecycle;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractTask extends Task implements WorkerJobLifecycle {
    private static final Logger log = LoggerFactory.getLogger(AbstractTask.class);

    @Schema(
        title = "MongoDB connection properties"
    )
    @NotNull
    @PluginProperty(group = "main")
    protected MongoDbConnection connection;

    @Schema(
        title = "MongoDB database"
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> database;

    @Schema(
        title = "MongoDB collection"
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> collection;

    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final transient AtomicReference<Runnable> killAction = new AtomicReference<>();

    /**
     * Creates the client of this run, and registers how {@link #kill()} stops its operations.
     */
    protected MongoClient client(RunContext runContext) throws IllegalVariableEvaluationException {
        MongoDbSessionKiller sessionKiller = new MongoDbSessionKiller(this.connection.settings(runContext));
        MongoClient mongoClient = sessionKiller.client();
        this.killAction.set(() ->
        {
            closeQuietly(mongoClient);
            sessionKiller.killSessions();
        });
        return mongoClient;
    }

    /**
     * Best-effort cancellation of the running operation, without blocking the calling worker thread.
     *
     * <p>
     * A background daemon thread closes the client, which stops further operations and server selection, then kills
     * the client's server sessions so that the server interrupts the in-flight operation instead of running it to
     * completion. Writes already applied are not rolled back. See {@link MongoDbSessionKiller} for when killing the
     * sessions is not possible.
     *
     * <p>
     * Only the first call does anything. A server that never answers can hold that daemon thread, which never keeps
     * JVM shutdown alive.
     */
    @Override
    public void kill() {
        // taking the action makes repeated or racing kills no-ops
        Runnable action = this.killAction.getAndSet(null);
        if (action == null) {
            return;
        }

        Thread killThread = new Thread(action, "kestra-mongodb-task-kill");
        killThread.setDaemon(true);
        killThread.start();
    }

    private static void closeQuietly(MongoClient mongoClient) {
        try {
            mongoClient.close();
        } catch (Exception e) {
            log.debug("Failed to close MongoDB client during kill()", e);
            // closing a client is idempotent, never fail kill()
        }
    }

    protected MongoCollection<Bson> collection(RunContext runContext, MongoClient client) throws IllegalVariableEvaluationException {
        return this.collection(runContext, client, Bson.class);
    }

    protected <T> MongoCollection<T> collection(RunContext runContext, MongoClient client, Class<T> cls) throws IllegalVariableEvaluationException {
        MongoDatabase database = client.getDatabase(runContext.render(this.database).as(String.class).orElseThrow());
        return database.getCollection(
            runContext.render(this.collection).as(String.class).orElseThrow(),
            cls
        );
    }
}
