package io.kestra.plugin.mongodb;

import java.util.List;
import java.util.Locale;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.not;

class InsertOneLoggingTest extends MongoDbContainer {

    @Test
    void shouldNotLogTheInsertedDocumentAtInfo() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Logger taskLogger = (Logger) runContext.logger();
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        taskLogger.addAppender(appender);

        try {
            InsertOne insert = InsertOne.builder()
                .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
                .database(Property.ofValue("ut_" + IdUtils.create().toLowerCase(Locale.ROOT)))
                .collection(Property.ofValue("users"))
                .document(ImmutableMap.of("name", "Jane", "ssn", "123-45-6789", "token", "SECRET-abc123"))
                .build();

            insert.run(runContext);
        } finally {
            taskLogger.detachAppender(appender);
        }

        List<String> infoOrAbove = appender.list.stream()
            .filter(event -> event.getLevel().isGreaterOrEqual(Level.INFO))
            .map(ILoggingEvent::getFormattedMessage)
            .toList();

        assertThat(infoOrAbove, everyItem(not(containsString("SECRET-abc123"))));
        assertThat(infoOrAbove, everyItem(not(containsString("123-45-6789"))));
    }
}
