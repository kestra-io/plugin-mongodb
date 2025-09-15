package io.kestra.plugin.mongodb;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.bson.Document;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Arrays;
import java.util.Date;
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
        
        loadTestData();
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

    private static void loadTestData() {
        try (MongoClient client = MongoClients.create("mongodb://localhost:27017")) {
            // Setup data for sales_db.transactions
            MongoDatabase salesDb = client.getDatabase("sales_db");
            MongoCollection<Document> transactions = salesDb.getCollection("transactions");
            transactions.drop();
            
            List<Document> transactionDocs = Arrays.asList(
                new Document("_id", 1)
                    .append("category", "Electronics")
                    .append("amount", 299.99)
                    .append("status", "completed"),
                new Document("_id", 2)
                    .append("category", "Electronics")
                    .append("amount", 199.99)
                    .append("status", "completed"),
                new Document("_id", 3)
                    .append("category", "Books")
                    .append("amount", 29.99)
                    .append("status", "completed"),
                new Document("_id", 4)
                    .append("category", "Books")
                    .append("amount", 19.99)
                    .append("status", "pending"),
                new Document("_id", 5)
                    .append("category", "Clothing")
                    .append("amount", 79.99)
                    .append("status", "completed")
            );
            transactions.insertMany(transactionDocs);
            
            // Setup data for analytics_db (users, orders, reviews)
            MongoDatabase analyticsDb = client.getDatabase("analytics_db");
            
            // Users collection
            MongoCollection<Document> users = analyticsDb.getCollection("users");
            users.drop();
            users.insertMany(Arrays.asList(
                new Document("_id", 1)
                    .append("name", "John Doe")
                    .append("email", "john@example.com")
                    .append("registrationDate", new Date()),
                new Document("_id", 2)
                    .append("name", "Jane Smith")
                    .append("email", "jane@example.com")
                    .append("registrationDate", new Date())
            ));
            
            // Orders collection
            MongoCollection<Document> orders = analyticsDb.getCollection("orders");
            orders.drop();
            orders.insertMany(Arrays.asList(
                new Document("orderId", 1)
                    .append("userId", 1)
                    .append("amount", 150.00),
                new Document("orderId", 2)
                    .append("userId", 1)
                    .append("amount", 200.00),
                new Document("orderId", 3)
                    .append("userId", 2)
                    .append("amount", 75.00)
            ));
            
            // Reviews collection
            MongoCollection<Document> reviews = analyticsDb.getCollection("reviews");
            reviews.drop();
            reviews.insertMany(Arrays.asList(
                new Document("reviewId", 1)
                    .append("userId", 1)
                    .append("rating", 5),
                new Document("reviewId", 2)
                    .append("userId", 1)
                    .append("rating", 4),
                new Document("reviewId", 3)
                    .append("userId", 2)
                    .append("rating", 5)
            ));
            
            // Setup data for reporting_db.sales
            MongoDatabase reportingDb = client.getDatabase("reporting_db");
            MongoCollection<Document> sales = reportingDb.getCollection("sales");
            sales.drop();
            sales.insertMany(Arrays.asList(
                new Document("_id", 1)
                    .append("amount", 100.00)
                    .append("customerId", "cust1")
                    .append("createdAt", new Date()),
                new Document("_id", 2)
                    .append("amount", 150.00)
                    .append("customerId", "cust2")
                    .append("createdAt", new Date())
            ));
            
            // Setup data for test_db.test_collection (for simple aggregate test)
            MongoDatabase testDb = client.getDatabase("test_db");
            MongoCollection<Document> testCollection = testDb.getCollection("test_collection");
            testCollection.drop();
            testCollection.insertMany(Arrays.asList(
                new Document("_id", 1)
                    .append("title", "Book One")
                    .append("author", "Author A")
                    .append("category", "Fiction")
                    .append("price", 25.99)
                    .append("quantity", 100)
                    .append("status", "available"),
                new Document("_id", 2)
                    .append("title", "Book Two")
                    .append("author", "Author B")
                    .append("category", "Fiction")
                    .append("price", 30.50)
                    .append("quantity", 50)
                    .append("status", "available"),
                new Document("_id", 3)
                    .append("title", "Book Three")
                    .append("author", "Author A")
                    .append("category", "Science")
                    .append("price", 45.00)
                    .append("quantity", 0)
                    .append("status", "out_of_stock"),
                new Document("_id", 4)
                    .append("title", "Book Four")
                    .append("author", "Author C")
                    .append("category", "Science")
                    .append("price", 35.99)
                    .append("quantity", 75)
                    .append("status", "available"),
                new Document("_id", 5)
                    .append("title", "Book Five")
                    .append("author", "Author B")
                    .append("category", "History")
                    .append("price", 28.00)
                    .append("quantity", 30)
                    .append("status", "available")
            ));
        }
    }
}