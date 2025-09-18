package io.kestra.plugin.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.kestra.core.junit.annotations.ExecuteFlow;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.State;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Runner test for MongoDB Aggregate task.
 * This test validates that the Aggregate task configuration can be parsed
 * and executed successfully as part of a Kestra flow.
 */
@KestraTest(startRunner = true)
class RunnerTest extends MongoDbContainer {

    @BeforeEach
    void setupTestData() {
        try (MongoClient client = MongoClients.create(connectionUri)) {
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
                    .append("status", "completed"),
                new Document("_id", 6)
                    .append("category", "Electronics")
                    .append("amount", 899.99)
                    .append("status", "completed"),
                new Document("_id", 7)
                    .append("category", "Home & Garden")
                    .append("amount", 149.99)
                    .append("status", "completed"),
                new Document("_id", 8)
                    .append("category", "Sports")
                    .append("amount", 89.99)
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
                    .append("registrationDate", new Date()),
                new Document("_id", 3)
                    .append("name", "Bob Johnson")
                    .append("email", "bob@example.com")
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
                    .append("amount", 75.00),
                new Document("orderId", 4)
                    .append("userId", 1)
                    .append("amount", 500.00),
                new Document("orderId", 5)
                    .append("userId", 2)
                    .append("amount", 450.00),
                new Document("orderId", 6)
                    .append("userId", 3)
                    .append("amount", 1200.00)
            ));

            // Reviews collection
            MongoCollection<Document> reviews = analyticsDb.getCollection("reviews");
            reviews.drop();
            reviews.insertMany(Arrays.asList(
                new Document("reviewId", 1)
                    .append("userId", 1)
                    .append("productId", 101)
                    .append("rating", 4.5),
                new Document("reviewId", 2)
                    .append("userId", 1)
                    .append("productId", 102)
                    .append("rating", 5.0),
                new Document("reviewId", 3)
                    .append("userId", 2)
                    .append("productId", 103)
                    .append("rating", 3.5),
                new Document("reviewId", 4)
                    .append("userId", 3)
                    .append("productId", 101)
                    .append("rating", 4.0)
            ));

            // Setup data for reporting_db.sales
            MongoDatabase reportingDb = client.getDatabase("reporting_db");
            MongoCollection<Document> sales = reportingDb.getCollection("sales");
            sales.drop();

            // Create dates for monthly revenue report
            Date jan2024 = new Date(124, 0, 15); // January 15, 2024
            Date feb2024 = new Date(124, 1, 20); // February 20, 2024
            Date mar2024 = new Date(124, 2, 10); // March 10, 2024

            sales.insertMany(Arrays.asList(
                new Document("_id", 1)
                    .append("createdAt", jan2024)
                    .append("amount", 1500.00)
                    .append("customerId", 1),
                new Document("_id", 2)
                    .append("createdAt", jan2024)
                    .append("amount", 2000.00)
                    .append("customerId", 2),
                new Document("_id", 3)
                    .append("createdAt", feb2024)
                    .append("amount", 1200.00)
                    .append("customerId", 1),
                new Document("_id", 4)
                    .append("createdAt", feb2024)
                    .append("amount", 800.00)
                    .append("customerId", 3),
                new Document("_id", 5)
                    .append("createdAt", mar2024)
                    .append("amount", 3000.00)
                    .append("customerId", 2),
                new Document("_id", 6)
                    .append("createdAt", mar2024)
                    .append("amount", 1800.00)
                    .append("customerId", 1)
            ));

            // Also setup test_db for the simple aggregation test
            MongoDatabase testDb = client.getDatabase("test_db");
            MongoCollection<Document> testCollection = testDb.getCollection("test_collection");
            testCollection.drop();
            testCollection.insertMany(Arrays.asList(
                new Document("_id", 1)
                    .append("name", "Test Document 1")
                    .append("value", 100),
                new Document("_id", 2)
                    .append("name", "Test Document 2")
                    .append("value", 200)
            ));
        }
    }
    
    /**
     * Test the simple aggregation pipeline flow.
     * This test verifies that the Aggregate task can be properly executed
     * with a basic aggregation pipeline.
     */
    @Test
    @ExecuteFlow("flows/sanity-checks/aggregate-simple.yml")
    void aggregateSimple(Execution execution) {
        assertThat(execution.getTaskRunList(), hasSize(1));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }
    
    /**
     * Test the comprehensive MongoDB aggregation example flow.
     * This test validates that complex aggregation pipelines with multiple stages,
     * lookups, transformations, and various configuration options can be executed.
     * 
     * The flow includes:
     * - Simple grouping and sorting operations
     * - Complex lookups with multiple collections
     * - Field transformations and conditional logic
     * - Time-based aggregations with dynamic parameters
     * - Various configuration options (allowDiskUse, maxTimeMs, store, batchSize)
     */
    @Test
    @ExecuteFlow("flows/mongo-aggregate-example.yml")
    void mongoAggregateExample(Execution execution) {
        assertThat(execution.getTaskRunList(), hasSize(3));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }
}