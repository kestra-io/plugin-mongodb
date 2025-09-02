package io.kestra.plugin.mongodb;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class AggregateTest {
    @Inject
    private RunContextFactory runContextFactory;

    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Aggregate aggregate = Aggregate.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.ofValue("mongodb://root:example@localhost:27017/?authSource=admin"))
                .build())
            .database(Property.ofValue("samples"))
            .collection(Property.ofValue("books"))
            .pipeline(Arrays.asList(
                ImmutableMap.of("$match", ImmutableMap.of(
                    "pageCount", ImmutableMap.of("$gt", 500)
                )),
                ImmutableMap.of("$group", ImmutableMap.of(
                    "_id", "$status",
                    "totalBooks", ImmutableMap.of("$sum", 1),
                    "avgPageCount", ImmutableMap.of("$avg", "$pageCount"),
                    "maxPageCount", ImmutableMap.of("$max", "$pageCount")
                )),
                ImmutableMap.of("$sort", ImmutableMap.of("totalBooks", -1))
            ))
            .build();

        Aggregate.Output output = aggregate.run(runContext);

        assertThat(output.getSize(), is(1L));
        assertThat(output.getRows(), hasSize(1));
        
        Map<String, Object> result = (Map<String, Object>) output.getRows().get(0);
        assertThat(result.get("_id"), is("PUBLISH"));
        assertThat(result.get("totalBooks"), is(59));
        assertThat(((Number) result.get("avgPageCount")).intValue(), is(646));
        assertThat(result.get("maxPageCount"), is(1101));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testGroupByCategories() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Aggregate aggregate = Aggregate.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.ofValue("mongodb://root:example@localhost:27017/?authSource=admin"))
                .build())
            .database(Property.ofValue("samples"))
            .collection(Property.ofValue("books"))
            .pipeline(Arrays.asList(
                ImmutableMap.of("$unwind", "$categories"),
                ImmutableMap.of("$group", ImmutableMap.of(
                    "_id", "$categories",
                    "count", ImmutableMap.of("$sum", 1)
                )),
                ImmutableMap.of("$sort", ImmutableMap.of("count", -1)),
                ImmutableMap.of("$limit", 5)
            ))
            .build();

        Aggregate.Output output = aggregate.run(runContext);

        assertThat(output.getSize(), is(5L));
        assertThat(output.getRows(), hasSize(5));
        
        Map<String, Object> topCategory = (Map<String, Object>) output.getRows().get(0);
        assertThat(topCategory.get("_id"), is("Java"));
        assertThat(topCategory.get("count"), is(95));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testProjection() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Aggregate aggregate = Aggregate.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.ofValue("mongodb://root:example@localhost:27017/?authSource=admin"))
                .build())
            .database(Property.ofValue("samples"))
            .collection(Property.ofValue("books"))
            .pipeline(Arrays.asList(
                ImmutableMap.of("$match", ImmutableMap.of(
                    "_id", ImmutableMap.of("$lte", 5)
                )),
                ImmutableMap.of("$project", ImmutableMap.of(
                    "title", 1,
                    "pageCount", 1,
                    "year", ImmutableMap.of("$year", "$publishedDate"),
                    "month", ImmutableMap.of("$month", "$publishedDate"),
                    "hasLongDescription", ImmutableMap.of("$gt", Arrays.asList(
                        ImmutableMap.of("$strLenCP", ImmutableMap.of("$ifNull", Arrays.asList("$longDescription", ""))),
                        500
                    ))
                )),
                ImmutableMap.of("$sort", ImmutableMap.of("_id", 1))
            ))
            .build();

        Aggregate.Output output = aggregate.run(runContext);

        assertThat(output.getSize(), is(5L));
        assertThat(output.getRows(), hasSize(5));
        
        // Check first book
        Map<String, Object> firstBook = (Map<String, Object>) output.getRows().get(0);
        assertThat(firstBook.get("_id"), is(1));
        assertThat(firstBook.get("title"), is("Unlocking Android"));
        assertThat(firstBook.get("pageCount"), is(416));
        assertThat(firstBook.get("year"), is(2009));
        assertThat(firstBook.get("month"), is(4));
        assertThat(firstBook.get("hasLongDescription"), is(true));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testWithStore() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Aggregate aggregate = Aggregate.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.ofValue("mongodb://root:example@localhost:27017/?authSource=admin"))
                .build())
            .database(Property.ofValue("samples"))
            .collection(Property.ofValue("books"))
            .pipeline(Arrays.asList(
                ImmutableMap.of("$group", ImmutableMap.of(
                    "_id", ImmutableMap.of(
                        "status", "$status",
                        "hasPages", ImmutableMap.of("$cond", Arrays.asList(
                            ImmutableMap.of("$gt", Arrays.asList("$pageCount", 0)),
                            "YES",
                            "NO"
                        ))
                    ),
                    "count", ImmutableMap.of("$sum", 1)
                ))
            ))
            .store(Property.ofValue(true))
            .build();

        Aggregate.Output output = aggregate.run(runContext);

        assertThat(output.getUri(), is(notNullValue()));
        assertThat(output.getRows(), is(nullValue()));
        assertThat(output.getSize(), greaterThan(0L));

        // Verify stored file content
        try (var inputStream = runContext.storage().getFile(output.getUri());
             var reader = new BufferedReader(new InputStreamReader(inputStream))) {
            List<Object> storedRows = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                storedRows.add(JacksonMapper.ofIon().readValue(line, Object.class));
            }
            assertThat(storedRows.size(), is(output.getSize().intValue()));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void testComplexPipeline() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Aggregate aggregate = Aggregate.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.ofValue("mongodb://root:example@localhost:27017/?authSource=admin"))
                .build())
            .database(Property.ofValue("samples"))
            .collection(Property.ofValue("books"))
            .pipeline(Arrays.asList(
                ImmutableMap.of("$match", ImmutableMap.of(
                    "authors", ImmutableMap.of("$exists", true, "$ne", Arrays.asList())
                )),
                ImmutableMap.of("$unwind", "$authors"),
                ImmutableMap.of("$group", ImmutableMap.of(
                    "_id", "$authors",
                    "bookCount", ImmutableMap.of("$sum", 1),
                    "avgPageCount", ImmutableMap.of("$avg", "$pageCount"),
                    "titles", ImmutableMap.of("$push", "$title")
                )),
                ImmutableMap.of("$match", ImmutableMap.of(
                    "bookCount", ImmutableMap.of("$gte", 3)
                )),
                ImmutableMap.of("$sort", ImmutableMap.of("bookCount", -1))
            ))
            .allowDiskUse(Property.ofValue(true))
            .build();

        Aggregate.Output output = aggregate.run(runContext);

        assertThat(output.getRows(), is(notNullValue()));
        assertThat(output.getSize(), greaterThan(0L));

        // Check that we have authors with multiple books
        Map<String, Object> topAuthor = (Map<String, Object>) output.getRows().get(0);
        assertThat(topAuthor.get("bookCount"), is(notNullValue()));
        assertThat(((Number) topAuthor.get("bookCount")).intValue(), greaterThanOrEqualTo(3));
        assertThat(topAuthor.get("titles"), instanceOf(List.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testStringPipeline() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Aggregate aggregate = Aggregate.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.ofValue("mongodb://root:example@localhost:27017/?authSource=admin"))
                .build())
            .database(Property.ofValue("samples"))
            .collection(Property.ofValue("books"))
            .pipeline(Arrays.asList(
                "{\"$match\": {\"status\": \"PUBLISH\"}}",
                "{\"$count\": \"totalPublished\"}"
            ))
            .build();

        Aggregate.Output output = aggregate.run(runContext);

        assertThat(output.getSize(), is(1L));
        assertThat(output.getRows(), hasSize(1));
        
        Map<String, Object> result = (Map<String, Object>) output.getRows().get(0);
        assertThat(result.get("totalPublished"), is(431));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testEmptyPipeline() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Aggregate aggregate = Aggregate.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.ofValue("mongodb://root:example@localhost:27017/?authSource=admin"))
                .build())
            .database(Property.ofValue("samples"))
            .collection(Property.ofValue("books"))
            .pipeline(new ArrayList<>())
            .build();

        Aggregate.Output output = aggregate.run(runContext);

        // Empty pipeline returns all documents
        assertThat(output.getSize(), is(431L));
        assertThat(output.getRows(), hasSize(431));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testWithTemporaryCollection() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String tempCollection = "temp_" + IdUtils.create().toLowerCase(Locale.ROOT);

        // First insert some test data
        InsertOne insert = InsertOne.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.ofValue("mongodb://root:example@localhost:27017/?authSource=admin"))
                .build())
            .database(Property.ofValue("samples"))
            .collection(Property.ofValue(tempCollection))
            .document(ImmutableMap.of(
                "type", "test",
                "value", 100
            ))
            .build();
        
        insert.run(runContext);

        // Now aggregate on the temporary collection
        Aggregate aggregate = Aggregate.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.ofValue("mongodb://root:example@localhost:27017/?authSource=admin"))
                .build())
            .database(Property.ofValue("samples"))
            .collection(Property.ofValue(tempCollection))
            .pipeline(Arrays.asList(
                ImmutableMap.of("$match", ImmutableMap.of("type", "test"))
            ))
            .build();

        Aggregate.Output output = aggregate.run(runContext);

        assertThat(output.getSize(), is(1L));
        assertThat(output.getRows(), hasSize(1));
        
        Map<String, Object> result = (Map<String, Object>) output.getRows().get(0);
        assertThat(result.get("type"), is("test"));
        assertThat(result.get("value"), is(100));

        // Clean up
        Delete delete = Delete.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.ofValue("mongodb://root:example@localhost:27017/?authSource=admin"))
                .build())
            .database(Property.ofValue("samples"))
            .collection(Property.ofValue(tempCollection))
            .filter(ImmutableMap.of())
            .build();
        
        delete.run(runContext);
    }
}