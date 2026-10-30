package io.kestra.plugin.mongodb;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

class OutputsSizeWarningTest extends MongoDbContainer {
    private static final String DATABASE = "ut_%s".formatted(IdUtils.create().toLowerCase(Locale.ROOT));
    private static final String COLLECTION = "big";
    private static final String TYPED_COLLECTION = "typed";

    // 1,100 documents of about 1,030 bytes: 1,100 of them pass 1 MiB, 900 stay under it.
    // 7,000 documents with ObjectIds and dates: about 850 KB as BSON but 1.3 MB as the JSON the queue measures.
    @BeforeAll
    void seed() {
        try (var client = getMongoClient()) {
            var payload = "x".repeat(1000);
            var documents = new ArrayList<Document>();
            for (var i = 0; i < 1100; i++) {
                documents.add(new Document("_id", i).append("payload", payload));
            }
            client.getDatabase(DATABASE).getCollection(COLLECTION).insertMany(documents);

            var typed = new ArrayList<Document>();
            for (var i = 0; i < 7000; i++) {
                typed.add(
                    new Document("_id", new ObjectId())
                        .append("userId", new ObjectId())
                        .append("createdAt", new Date(1_700_000_000_000L))
                        .append("updatedAt", new Date(1_700_000_100_000L))
                        .append("amount", 1234.5678)
                        .append("status", "ok")
                        .append("count", 42)
                );
            }
            client.getDatabase(DATABASE).getCollection(TYPED_COLLECTION).insertMany(typed);
        }
    }

    @Test
    void findWarnsWhenTheJsonOfTheRowsPassesOneMiB() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var find = find().collection(Property.ofValue(TYPED_COLLECTION)).build();

        var warnings = warnings(runContext, () ->
        {
            assertThat(find.run(runContext).getRows(), hasSize(7000));
            return null;
        });

        assertThat(warnings, contains(allOf(containsString("7000 rows"), containsString("store: true"))));
    }

    @Test
    void findWarnsWhenTheRowsKeptInTheOutputsPassOneMiB() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var find = find().build();

        var warnings = warnings(runContext, () ->
        {
            assertThat(find.run(runContext).getRows(), hasSize(1100));
            return null;
        });

        assertThat(warnings, contains(allOf(containsString("1100 rows"), containsString("store: true"))));
    }

    @Test
    void findDoesNotWarnUnderOneMiBOrWhenStoring() throws Exception {
        var runContext = runContextFactory.of(Map.of());

        var under = warnings(runContext, () -> find().limit(Property.ofValue(900)).build().run(runContext));
        var stored = warnings(runContext, () -> find().store(Property.ofValue(true)).build().run(runContext));

        assertThat(under, is(empty()));
        assertThat(stored, is(empty()));
    }

    @Test
    void aggregateWarnsWhenFetchedRowsPassOneMiB() throws Exception {
        var runContext = runContextFactory.of(Map.of());

        var fetched = warnings(runContext, () -> aggregate().build().run(runContext));
        var stored = warnings(runContext, () -> aggregate().store(Property.ofValue(FetchType.STORE)).build().run(runContext));

        assertThat(fetched, contains(allOf(containsString("1100 rows"), containsString("store: STORE"))));
        assertThat(stored, is(empty()));
    }

    @Test
    void triggerWarnsWhenItsRowsPassOneMiB() throws Exception {
        var trigger = Trigger.builder()
            .id("watch_%s".formatted(IdUtils.create()))
            .type(Trigger.class.getName())
            .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .build();
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        var warnings = warnings(context.getKey().getRunContext(), () -> trigger.evaluate(context.getKey(), context.getValue()));

        assertThat(warnings, contains(allOf(containsString("1100 rows"), containsString("store: true"))));
    }

    private Find.FindBuilder<?, ?> find() {
        return Find.builder()
            .id(IdUtils.create())
            .type(Find.class.getName())
            .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION));
    }

    private Aggregate.AggregateBuilder<?, ?> aggregate() {
        return Aggregate.builder()
            .id(IdUtils.create())
            .type(Aggregate.class.getName())
            .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(DATABASE))
            .collection(Property.ofValue(COLLECTION))
            .pipeline(Property.ofValue(List.of(Map.of("$sort", Map.of("_id", 1)))));
    }

    private static List<String> warnings(RunContext runContext, Callable<?> action) throws Exception {
        var logger = (Logger) runContext.logger();
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        logger.addAppender(appender);

        try {
            action.call();
        } finally {
            logger.detachAppender(appender);
        }

        return appender.list.stream()
            .filter(event -> event.getLevel() == Level.WARN)
            .map(ILoggingEvent::getFormattedMessage)
            .toList();
    }
}
