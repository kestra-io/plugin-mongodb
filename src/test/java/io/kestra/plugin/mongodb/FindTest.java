package io.kestra.plugin.mongodb;

import com.google.common.collect.ImmutableMap;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class FindTest extends MongoDbContainer {

    @BeforeEach
    void setUp() {
        // Set up test data for samples.books collection
        try (MongoClient client = MongoClients.create(connectionUri)) {
            MongoDatabase database = client.getDatabase("samples");
            MongoCollection<Document> collection = database.getCollection("books");

            // Clear existing data
            collection.drop();

            // Insert test documents - create 36 books with pageCount > 600
            List<Document> books = new ArrayList<>();

            // First book with highest pageCount (1101)
            books.add(new Document("_id", 70)
                .append("title", "Advanced Java Programming")
                .append("pageCount", 1101)
                .append("publishedDate", Instant.parse("2000-08-01T07:00:00Z")));

            // Second book
            books.add(new Document("_id", 315)
                .append("title", "Database Systems")
                .append("pageCount", 950)
                .append("publishedDate", Instant.parse("2001-05-15T08:00:00Z")));

            // Add 34 more books with pageCount > 600 (decreasing order by pageCount)
            for (int i = 1; i <= 34; i++) {
                books.add(new Document("_id", i)
                    .append("title", "Book " + i)
                    .append("pageCount", 900 - (i * 5)) // Starts at 895, decreases by 5 each time, all > 600
                    .append("publishedDate", Instant.parse("200" + (i % 9 + 1) + "-01-01T00:00:00Z")));
            }

            // Also add some books with pageCount <= 600 to ensure filter works
            for (int i = 100; i < 110; i++) {
                books.add(new Document("_id", i)
                    .append("title", "Small Book " + i)
                    .append("pageCount", 300 + (i % 10) * 10) // 300-390 pages
                    .append("publishedDate", Instant.parse("2010-01-01T00:00:00Z")));
            }

            collection.insertMany(books);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Find find = Find.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.ofValue(connectionUri))
                .build())
            .database(Property.ofValue("samples"))
            .collection(Property.ofValue("books"))
            .filter(ImmutableMap.of(
                "pageCount", ImmutableMap.of("$gt", 600)
            ))
            .sort(ImmutableMap.of(
                "pageCount", -1
            ))
            .projection(ImmutableMap.of(
                "title", 1,
                "publishedDate", 1,
                "pageCount", 1
            ))
            .build();

        Find.Output findOutput = find.run(runContext);

        assertThat(findOutput.getSize(), is(36L));
        assertThat(((Map<String, Object>) findOutput.getRows().get(0)).get("pageCount"), is(1101));
        assertThat(((Map<String, Object>) findOutput.getRows().get(0)).get("_id"), is(70));
        assertThat(((Map<String, Object>) findOutput.getRows().get(0)).size(), is(4));
        assertThat(((Map<String, Object>) findOutput.getRows().get(0)).get("publishedDate"), is(Instant.parse("2000-08-01T07:00:00Z")));

        assertThat(((Map<String, Object>) findOutput.getRows().get(1)).get("_id"), is(315));
    }

    @Test
    void shouldPreserveFieldOrder() throws Exception {
        RunContext runContext = runContextFactory.of();

        var insertOneOutput = InsertOne.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.ofValue(connectionUri))
                .build())
            .database(Property.ofValue("samples"))
            .collection(Property.ofValue("books"))
            .document("{\"a\": 1, \"z\": 2, \"m\": 3}")
            .build()
            .run(runContext);

        assertThat(insertOneOutput.getInsertedId(), is(notNullValue()));

        Find find = Find.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.ofValue(connectionUri))
                .build())
            .database(Property.ofValue("samples"))
            .collection(Property.ofValue("books"))
            .filter(Map.of(
                "_id", Map.of("$oid", insertOneOutput.getInsertedId())
            ))
            .store(Property.ofValue(false))
            .build();

        Find.Output output = find.run(runContext);

        System.out.println(output.getRows() + " " + output.getSize());
        assertThat(output.getSize(), is(1L));

        Map<String, Object> doc = (Map<String, Object>) output.getRows().get(0);
        List<String> keys = new ArrayList<>(doc.keySet());
        System.out.println(keys);
        assertThat(keys, contains("_id", "a", "z", "m"));
    }
}
