package io.kestra.plugin.mongodb;

import java.util.Map;

import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContextFactory;

import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;

/**
 * Starts one MongoDB container for the whole test run and passes its URI to the test flows, which
 * connect with {@code uri: "{{ globals['mongodb-uri'] }}"}.
 *
 * <p>
 * {@link TestPropertyProvider} needs one test instance per class, so that the properties are known before
 * the application context starts.
 */
@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MongoDbContainer implements TestPropertyProvider {

    private static final MongoDBContainer mongoDBContainer;
    protected static String connectionUri;

    static {
        mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:8.0"));

        mongoDBContainer.start();
        connectionUri = mongoDBContainer.getConnectionString();
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            if (mongoDBContainer != null) {
                mongoDBContainer.stop();
            }
        }));
    }

    @Inject
    protected RunContextFactory runContextFactory;

    @Override
    public Map<String, String> getProperties() {
        return Map.of("kestra.variables.globals.mongodb-uri", connectionUri);
    }

    /**
     * Get a MongoClient connected to the test container.
     * Tests should use this to set up their own test data.
     * Remember to close the client when done.
     */
    protected static MongoClient getMongoClient() {
        return MongoClients.create(connectionUri);
    }
}
