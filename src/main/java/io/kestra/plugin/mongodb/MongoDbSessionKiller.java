package io.kestra.plugin.mongodb;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;

/**
 * Records the server sessions used by one MongoDB client, so that a killed task or trigger can stop its operations on
 * the server.
 *
 * <p>
 * Closing a client does not stop an operation the server is already running: the blocked driver call only returns
 * once the server is done, and a write keeps modifying documents until then. The driver sends a session id
 * ({@code lsid}) with every command; {@link #killSessions()} sends {@code killSessions} for the recorded ids, which
 * makes the server interrupt their in-progress operations and close their cursors, so the blocked call fails promptly.
 *
 * <p>
 * Writes applied before the kill are not rolled back. If the deployment does not support {@code killSessions} (for
 * example Atlas M0 and Flex clusters), the command fails and only closing the client remains.
 */
final class MongoDbSessionKiller implements CommandListener {
    private static final Logger log = LoggerFactory.getLogger(MongoDbSessionKiller.class);

    private final MongoClientSettings settings;
    private final Set<BsonDocument> sessionIds = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean killed = new AtomicBoolean();

    MongoDbSessionKiller(MongoClientSettings settings) {
        this.settings = settings;
    }

    /**
     * Creates a client whose sessions are recorded by this instance.
     */
    MongoClient client() {
        return MongoClients.create(
            MongoClientSettings.builder(this.settings)
                .addCommandListener(this)
                .build()
        );
    }

    @Override
    public void commandStarted(CommandStartedEvent event) {
        // the command document is only readable during this callback, the binary id is a copy
        if (event.getCommand().get("lsid") instanceof BsonDocument lsid && lsid.get("id") instanceof BsonBinary id) {
            this.sessionIds.add(new BsonDocument("id", id));
        }
    }

    /**
     * Sends {@code killSessions} for the recorded sessions; only the first call does anything.
     *
     * <p>
     * The command goes through a short-lived client built from the same settings, so it authenticates as the same user
     * (a user can only kill their own sessions) and does not depend on the killed client, which may already be closed
     * or have no free connection.
     *
     * <p>
     * This is a blocking round trip with no client-side timeout: call it from a background thread, never from the
     * worker thread. It never throws.
     */
    void killSessions() {
        if (!this.killed.compareAndSet(false, true)) {
            return;
        }

        List<BsonDocument> ids = List.copyOf(this.sessionIds);
        // an empty array would kill every session of the user, not none
        if (ids.isEmpty()) {
            return;
        }

        try (MongoClient killer = MongoClients.create(this.settings)) {
            killer.getDatabase("admin").runCommand(new BsonDocument("killSessions", new BsonArray(ids)));
        } catch (Exception e) {
            log.debug("Failed to kill MongoDB sessions during kill()", e);
            // killing sessions is best-effort, never fail kill()
        }
    }
}
