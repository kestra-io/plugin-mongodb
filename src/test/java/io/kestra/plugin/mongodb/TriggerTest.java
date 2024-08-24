package io.kestra.plugin.mongodb;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.Worker;
import io.kestra.core.schedulers.AbstractScheduler;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
public class TriggerTest {

    @Inject
    private ApplicationContext applicationContext;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

    @Inject
    private FlowListeners flowListenersService;

    @Inject
    private LocalFlowRepositoryLoader localFlowRepositoryLoader;

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
            Worker worker = applicationContext.createBean(Worker.class, IdUtils.create(), 8, null)
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
