package io.kestra.plugin.mongodb;

import com.google.common.collect.ImmutableMap;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.junit.annotations.KestraTest;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class AggregateTest extends MongoDbContainer {
    private static final String DATABASE_NAME = "test_db";
    private static final String COLLECTION_NAME = "test_collection";


    @SuppressWarnings("unchecked")
    @Test
    void testSimpleAggregation() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Aggregate aggregate = Aggregate.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.ofValue(connectionUri))
                .build())
            .database(Property.ofValue(DATABASE_NAME))
            .collection(Property.ofValue(COLLECTION_NAME))
            .pipeline(Property.ofValue(Arrays.asList(
                ImmutableMap.of("$match", ImmutableMap.of(
                    "status", "available"
                )),
                ImmutableMap.of("$group", ImmutableMap.of(
                    "_id", "$category",
                    "totalBooks", ImmutableMap.of("$sum", 1),
                    "avgPrice", ImmutableMap.of("$avg", "$price"),
                    "totalQuantity", ImmutableMap.of("$sum", "$quantity")
                )),
                ImmutableMap.of("$sort", ImmutableMap.of("totalBooks", -1))
            )))
            .build();

        Aggregate.Output output = aggregate.run(runContext);

        assertThat(output.getSize(), is(3L));
        assertThat(output.getRows(), hasSize(3));
        
        Map<String, Object> result = (Map<String, Object>) output.getRows().get(0);
        assertThat(result.get("_id"), is("Fiction"));
        assertThat(result.get("totalBooks"), is(2));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testGroupByAuthor() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Aggregate aggregate = Aggregate.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.ofValue(connectionUri))
                .build())
            .database(Property.ofValue(DATABASE_NAME))
            .collection(Property.ofValue(COLLECTION_NAME))
            .pipeline(Property.ofValue(Arrays.asList(
                ImmutableMap.of("$group", ImmutableMap.of(
                    "_id", "$author",
                    "bookCount", ImmutableMap.of("$sum", 1),
                    "titles", ImmutableMap.of("$push", "$title")
                )),
                ImmutableMap.of("$sort", ImmutableMap.of("bookCount", -1))
            )))
            .build();

        Aggregate.Output output = aggregate.run(runContext);

        assertThat(output.getSize(), is(3L));
        assertThat(output.getRows(), hasSize(3));
        
        // Author A and Author B both have 2 books
        Map<String, Object> topAuthor = (Map<String, Object>) output.getRows().get(0);
        assertThat(topAuthor.get("bookCount"), is(2));
        assertThat(topAuthor.get("titles"), instanceOf(List.class));
        assertThat(((List<?>) topAuthor.get("titles")).size(), is(2));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testProjection() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Aggregate aggregate = Aggregate.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.ofValue(connectionUri))
                .build())
            .database(Property.ofValue(DATABASE_NAME))
            .collection(Property.ofValue(COLLECTION_NAME))
            .pipeline(Property.ofValue(Arrays.asList(
                ImmutableMap.of("$project", ImmutableMap.of(
                    "title", 1,
                    "discountedPrice", ImmutableMap.of("$multiply", Arrays.asList("$price", 0.9)),
                    "inStock", ImmutableMap.of("$gt", Arrays.asList("$quantity", 0))
                )),
                ImmutableMap.of("$sort", ImmutableMap.of("_id", 1))
            )))
            .build();

        Aggregate.Output output = aggregate.run(runContext);

        assertThat(output.getSize(), is(5L));
        assertThat(output.getRows(), hasSize(5));
        
        Map<String, Object> firstBook = (Map<String, Object>) output.getRows().get(0);
        assertThat(firstBook.get("_id"), is(1));
        assertThat(firstBook.get("title"), is("Book One"));
        assertThat(firstBook.get("inStock"), is(true));
        assertThat(((Number) firstBook.get("discountedPrice")).doubleValue(), 
                   closeTo(23.391, 0.001));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testWithStore() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Aggregate aggregate = Aggregate.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.ofValue(connectionUri))
                .build())
            .database(Property.ofValue(DATABASE_NAME))
            .collection(Property.ofValue(COLLECTION_NAME))
            .pipeline(Property.ofValue(Arrays.asList(
                ImmutableMap.of("$group", ImmutableMap.of(
                    "_id", "$status",
                    "count", ImmutableMap.of("$sum", 1),
                    "avgPrice", ImmutableMap.of("$avg", "$price")
                ))
            )))
            .store(Property.ofValue(true))
            .build();

        Aggregate.Output output = aggregate.run(runContext);

        assertThat(output.getUri(), is(notNullValue()));
        assertThat(output.getRows(), is(nullValue()));
        assertThat(output.getSize(), is(2L));

        // Verify stored file content
        try (var inputStream = runContext.storage().getFile(output.getUri());
             var reader = new BufferedReader(new InputStreamReader(inputStream))) {
            List<Object> storedRows = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                storedRows.add(JacksonMapper.ofIon().readValue(line, Object.class));
            }
            assertThat(storedRows.size(), is(2));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void testComplexPipeline() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Aggregate aggregate = Aggregate.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.ofValue(connectionUri))
                .build())
            .database(Property.ofValue(DATABASE_NAME))
            .collection(Property.ofValue(COLLECTION_NAME))
            .pipeline(Property.ofValue(Arrays.<Map<String, Object>>asList(
                ImmutableMap.of("$match", ImmutableMap.of(
                    "quantity", ImmutableMap.of("$gt", 0)
                )),
                ImmutableMap.of("$addFields", ImmutableMap.of(
                    "totalValue", ImmutableMap.of("$multiply", Arrays.asList("$price", "$quantity"))
                )),
                ImmutableMap.of("$group", ImmutableMap.of(
                    "_id", "$category",
                    "totalInventoryValue", ImmutableMap.of("$sum", "$totalValue"),
                    "avgPrice", ImmutableMap.of("$avg", "$price"),
                    "products", ImmutableMap.of("$push", ImmutableMap.of(
                        "title", "$title",
                        "value", "$totalValue"
                    ))
                )),
                ImmutableMap.of("$sort", ImmutableMap.of("totalInventoryValue", -1))
            )))
            .allowDiskUse(Property.ofValue(true))
            .build();

        Aggregate.Output output = aggregate.run(runContext);

        assertThat(output.getRows(), is(notNullValue()));
        assertThat(output.getSize(), greaterThan(0L));

        Map<String, Object> topCategory = (Map<String, Object>) output.getRows().get(0);
        assertThat(topCategory.get("_id"), is(notNullValue()));
        assertThat(topCategory.get("totalInventoryValue"), is(notNullValue()));
        assertThat(topCategory.get("products"), instanceOf(List.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testEmptyPipeline() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Aggregate aggregate = Aggregate.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.ofValue(connectionUri))
                .build())
            .database(Property.ofValue(DATABASE_NAME))
            .collection(Property.ofValue(COLLECTION_NAME))
            .pipeline(Property.ofValue(new ArrayList<>()))
            .build();

        Aggregate.Output output = aggregate.run(runContext);

        // Empty pipeline returns all documents
        assertThat(output.getSize(), is(5L));
        assertThat(output.getRows(), hasSize(5));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testWithLookup() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        
        // Create a second collection for lookup
        String ordersCollection = "orders";
        try (MongoClient client = MongoClients.create(connectionUri)) {
            MongoDatabase database = client.getDatabase(DATABASE_NAME);
            MongoCollection<Document> orders = database.getCollection(ordersCollection);
            
            orders.drop();
            orders.insertMany(Arrays.asList(
                new Document("orderId", 1).append("bookId", 1).append("quantity", 2),
                new Document("orderId", 2).append("bookId", 1).append("quantity", 1),
                new Document("orderId", 3).append("bookId", 2).append("quantity", 3),
                new Document("orderId", 4).append("bookId", 3).append("quantity", 1)
            ));
        }

        Aggregate aggregate = Aggregate.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.ofValue(connectionUri))
                .build())
            .database(Property.ofValue(DATABASE_NAME))
            .collection(Property.ofValue(COLLECTION_NAME))
            .pipeline(Property.ofValue(Arrays.asList(
                ImmutableMap.of("$lookup", ImmutableMap.of(
                    "from", ordersCollection,
                    "localField", "_id",
                    "foreignField", "bookId",
                    "as", "orders"
                )),
                ImmutableMap.of("$project", ImmutableMap.of(
                    "title", 1,
                    "orderCount", ImmutableMap.of("$size", "$orders")
                )),
                ImmutableMap.of("$match", ImmutableMap.of(
                    "orderCount", ImmutableMap.of("$gt", 0)
                )),
                ImmutableMap.of("$sort", ImmutableMap.of("orderCount", -1))
            )))
            .build();

        Aggregate.Output output = aggregate.run(runContext);

        assertThat(output.getSize(), is(3L));
        
        Map<String, Object> mostOrdered = (Map<String, Object>) output.getRows().get(0);
        assertThat(mostOrdered.get("title"), is("Book One"));
        assertThat(mostOrdered.get("orderCount"), is(2));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testWithMaxTimeMs() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Aggregate aggregate = Aggregate.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.ofValue(connectionUri))
                .build())
            .database(Property.ofValue(DATABASE_NAME))
            .collection(Property.ofValue(COLLECTION_NAME))
            .pipeline(Property.ofValue(Arrays.asList(
                ImmutableMap.of("$match", ImmutableMap.of("status", "available"))
            )))
            .maxTimeMs(Property.ofValue(5000)) // 5 seconds timeout
            .batchSize(Property.ofValue(2))
            .build();

        Aggregate.Output output = aggregate.run(runContext);

        assertThat(output.getSize(), is(4L));
        assertThat(output.getRows(), hasSize(4));
    }
}