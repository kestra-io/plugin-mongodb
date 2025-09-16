package io.kestra.plugin.mongodb;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

@KestraTest
public class MongoDbContainer {

    private static final MongoDBContainer mongoDBContainer;
    protected static String connectionUri;

    static {
        mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:8.0"))
            .withExposedPorts(27017)
            .withCreateContainerCmdModifier(cmd -> cmd.withHostConfig(
                cmd.getHostConfig().withPortBindings(
                    List.of(new PortBinding(
                        Ports.Binding.bindPort(27017),
                        new ExposedPort(27017)
                    ))
                )
            ));

        mongoDBContainer.start();
        connectionUri = "mongodb://localhost:27017";

        // Set system property for the connection URI that the flows can use
        System.setProperty("mongodb.uri", connectionUri);
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (mongoDBContainer != null) {
                mongoDBContainer.stop();
            }
            System.clearProperty("mongodb.uri");
        }));
    }

    @Inject
    protected RunContextFactory runContextFactory;

    /**
     * Get a MongoClient connected to the test container.
     * Tests should use this to set up their own test data.
     * Remember to close the client when done.
     */
    protected static MongoClient getMongoClient() {
        return MongoClients.create(connectionUri);
    }
}