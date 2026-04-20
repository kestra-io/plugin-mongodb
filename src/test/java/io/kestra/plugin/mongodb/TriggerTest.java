package io.kestra.plugin.mongodb;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import io.kestra.core.junit.annotations.EvaluateTrigger;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
public class TriggerTest extends MongoDbContainer {

    @BeforeEach
    void setUp() {
        // Set up test data for samples.books collection
        try (MongoClient client = MongoClients.create(connectionUri)) {
            MongoDatabase database = client.getDatabase("samples");
            MongoCollection<Document> collection = database.getCollection("books");

            // Clear existing data
            collection.drop();

            // Insert test documents - need to create 265 books with pageCount > 50
            List<Document> books = new ArrayList<>();

            // First book with highest pageCount (1101)
            books.add(
                new Document("_id", 70)
                    .append("title", "Essential Guide to Peoplesoft Development and Customization")
                    .append("pageCount", 1101)
                    .append("publishedDate", Instant.parse("2000-08-01T07:00:00Z"))
            );

            // Second book
            books.add(
                new Document("_id", 315)
                    .append("title", "Database Systems")
                    .append("pageCount", 950)
                    .append("publishedDate", Instant.parse("2001-05-15T08:00:00Z"))
            );

            // Add 263 more books with pageCount > 50
            for (int i = 1; i <= 263; i++) {
                int pageCount = 900 - (i * 2);
                if (pageCount <= 50) {
                    pageCount = 51 + (i % 100);
                }
                books.add(
                    new Document("_id", 1000 + i)
                        .append("title", "Book " + i)
                        .append("pageCount", pageCount)
                        .append("publishedDate", Instant.parse("200" + (i % 9 + 1) + "-01-01T00:00:00Z"))
                );
            }

            collection.insertMany(books);
        }
    }

    @Test
    @EvaluateTrigger(flow = "flows/mongo-listen.yml", triggerId = "watch")
    void run(Optional<Execution> optionalExecution) {
        assertThat(optionalExecution.isPresent(), is(true));
        Execution execution = optionalExecution.get();

        var rows = (List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows");

        assertThat(rows.size(), is(265));
        assertThat(rows.getFirst().size(), is(4));
        assertThat(rows.getFirst().get("pageCount"), is(1101));
        assertThat(rows.getFirst().get("_id"), is(70));
        assertThat(rows.getFirst().get("publishedDate"), is("2000-08-01T07:00:00Z"));
        assertThat(rows.getFirst().get("title"), is("Essential Guide to Peoplesoft Development and Customization"));
        assertThat(rows.get(1).get("_id"), is(315));
    }
}
