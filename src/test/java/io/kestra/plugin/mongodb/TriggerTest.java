package io.kestra.plugin.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.kestra.core.junit.annotations.KestraTest;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.Worker;
import io.kestra.scheduler.AbstractScheduler;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.kestra.worker.DefaultWorker;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
public class TriggerTest extends MongoDbContainer {

    @Inject
    private ApplicationContext applicationContext;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

    @Inject
    private FlowListeners flowListenersService;

    @Inject
    private LocalFlowRepositoryLoader localFlowRepositoryLoader;

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
            books.add(new Document("_id", 70)
                .append("title", "Essential Guide to Peoplesoft Development and Customization")
                .append("pageCount", 1101)
                .append("publishedDate", Instant.parse("2000-08-01T07:00:00Z")));

            // Second book
            books.add(new Document("_id", 315)
                .append("title", "Database Systems")
                .append("pageCount", 950)
                .append("publishedDate", Instant.parse("2001-05-15T08:00:00Z")));

            // Add 263 more books with pageCount > 50
            for (int i = 1; i <= 263; i++) {
                int pageCount = 900 - (i * 2);
                if (pageCount <= 50) {
                    pageCount = 51 + (i % 100);
                }
                books.add(new Document("_id", 1000 + i)
                    .append("title", "Book " + i)
                    .append("pageCount", pageCount)
                    .append("publishedDate", Instant.parse("200" + (i % 9 + 1) + "-01-01T00:00:00Z")));
            }

            collection.insertMany(books);
        }
    }

    @Test
    void run() throws Exception {
        Execution execution = triggerFlow();

        var rows = (List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows");

        assertThat(rows.size(), is(265));
        assertThat(rows.getFirst().size(), is(4));
        assertThat(rows.getFirst().get("pageCount"), is(1101));
        assertThat(rows.getFirst().get("_id"), is(70));
        assertThat(rows.getFirst().get("publishedDate"), is("2000-08-01T07:00:00Z"));
        assertThat(rows.getFirst().get("title"), is("Essential Guide to Peoplesoft Development and Customization"));

        assertThat(rows.get(1).get("_id"), is(315));
    }

    protected Execution triggerFlow() throws Exception {
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        try (
            AbstractScheduler scheduler = new JdbcScheduler(
                this.applicationContext,
                this.flowListenersService
            );
            DefaultWorker worker = applicationContext.createBean(DefaultWorker.class, UUID.randomUUID().toString(), 8, null);
        ) {
            // wait for execution
            Flux<Execution> receive = TestsUtils.receive(executionQueue, execution -> {
                queueCount.countDown();
                assertThat(execution.getLeft().getFlowId(), is("mongo-listen"));
            });

            worker.run();
            scheduler.run();

            localFlowRepositoryLoader.load(
                Objects.requireNonNull(
                    this.getClass()
                        .getClassLoader()
                        .getResource("flows/mongo-listen.yml")
                )
            );

            boolean await = queueCount.await(1, TimeUnit.MINUTES);
            assertThat(await, is(true));

            return receive.blockLast();
        }
    }
}
