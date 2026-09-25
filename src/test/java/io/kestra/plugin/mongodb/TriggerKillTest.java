package io.kestra.plugin.mongodb;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Deterministic unit tests for {@link Trigger#kill()}.
 *
 * <p>
 * No MongoDB container is needed: the in-flight cursor/client references are
 * injected directly, mirroring the state {@link Trigger#evaluate} sets before
 * a blocking driver call.
 */
class TriggerKillTest {
    @Test
    void killBeforeEvaluateIsNoop() {
        Trigger trigger = Trigger.builder().id("watch").build();

        trigger.kill();
        trigger.kill();
    }

    @Test
    void killAfterEvaluateCompletionIsNoop() throws Exception {
        Trigger trigger = Trigger.builder().id("watch").build();

        // completed evaluations clear both references
        assertThat(getField(trigger, "client"), nullValue());
        assertThat(getField(trigger, "cursor"), nullValue());

        trigger.kill();
    }

    @Test
    void killClosesClientAndIsIdempotent() throws Exception {
        Trigger trigger = Trigger.builder().id("watch").build();
        AtomicBoolean closed = new AtomicBoolean();
        MongoClient fakeClient = (MongoClient) Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] { MongoClient.class },
            (proxy, method, args) ->
            {
                switch (method.getName()) {
                    case "close":
                        closed.set(true);
                        return null;
                    case "toString":
                        return "fakeMongoClient";
                    case "hashCode":
                        return System.identityHashCode(proxy);
                    case "equals":
                        return proxy == args[0];
                    default:
                        throw new UnsupportedOperationException(method.getName());
                }
            }
        );
        setField(trigger, "client", fakeClient);

        trigger.kill();

        assertThat(closed.get(), is(true));

        // late / repeated kills stay safe
        trigger.kill();
        setField(trigger, "client", null);
        trigger.kill();
    }

    @Test
    void killUnblocksCursorAndIsIdempotent() throws Exception {
        Trigger trigger = Trigger.builder().id("watch").build();
        BlockingCursor cursor = new BlockingCursor();
        setField(trigger, "cursor", cursor);

        AtomicReference<Throwable> readerError = new AtomicReference<>();
        CountDownLatch readerDone = new CountDownLatch(1);
        Thread reader = new Thread(() ->
        {
            try {
                cursor.hasNext();
            } catch (Throwable t) {
                readerError.set(t);
            } finally {
                readerDone.countDown();
            }
        });
        reader.setDaemon(true);
        reader.start();

        assertThat(cursor.enteredHasNext.await(10, TimeUnit.SECONDS), is(true));

        trigger.kill();
        trigger.kill(); // repeated kill must stay safe

        assertThat(readerDone.await(10, TimeUnit.SECONDS), is(true));
        assertThat(cursor.closeCalls.get(), greaterThanOrEqualTo(1));
        assertThat(readerError.get(), instanceOf(IllegalStateException.class));
    }

    @Test
    void inFlightStateIsExcludedFromToString() throws Exception {
        Trigger trigger = Trigger.builder().id("watch").build();
        setField(trigger, "cursor", new BlockingCursor());

        assertThat(trigger.toString(), not(containsString("cursor=")));
        assertThat(trigger.toString(), not(containsString("client=")));
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = Trigger.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object getField(Object target, String name) throws Exception {
        Field field = Trigger.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    /**
     * Fake cursor blocked in {@link #hasNext()} until {@link #close()} is called,
     * mirroring a driver cursor stuck in a server round-trip.
     */
    private static final class BlockingCursor implements MongoCursor<BsonDocument> {
        final AtomicInteger closeCalls = new AtomicInteger();
        final AtomicBoolean closed = new AtomicBoolean();
        final CountDownLatch enteredHasNext = new CountDownLatch(1);

        @Override
        public void close() {
            closeCalls.incrementAndGet();
            closed.set(true);
        }

        @Override
        public boolean hasNext() {
            enteredHasNext.countDown();
            while (!closed.get()) {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted", e);
                }
            }
            throw new IllegalStateException("cursor closed");
        }

        @Override
        public BsonDocument next() {
            throw new NoSuchElementException("no elements");
        }

        @Override
        public int available() {
            return 0;
        }

        @Override
        public BsonDocument tryNext() {
            return null;
        }

        @Override
        public ServerCursor getServerCursor() {
            return null;
        }

        @Override
        public ServerAddress getServerAddress() {
            return new ServerAddress();
        }
    }
}
