package io.kestra.plugin.mongodb;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

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

        // closing happens on a background thread, wait for it without assuming timing
        assertThat(awaitTrue(closed::get), is(true));

        // late / repeated kills stay safe
        trigger.kill();
        setField(trigger, "client", null);
        trigger.kill();
    }

    @Test
    void killClosesCursorAndIsIdempotent() throws Exception {
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
    void killDoesNotBlockWhenCursorCloseHangs() throws Exception {
        Trigger trigger = Trigger.builder().id("watch").build();
        HangingCloseCursor cursor = new HangingCloseCursor();
        setField(trigger, "cursor", cursor);

        AtomicReference<Throwable> killError = new AtomicReference<>();
        CountDownLatch killDone = new CountDownLatch(1);
        Thread killer = new Thread(() ->
        {
            try {
                trigger.kill();
            } catch (Throwable t) {
                killError.set(t);
            } finally {
                killDone.countDown();
            }
        });
        killer.setDaemon(true);
        killer.start();

        try {
            assertThat("kill() must return without waiting for close()", killDone.await(10, TimeUnit.SECONDS), is(true));
            assertThat(killError.get(), nullValue());
            assertThat("close must still be attempted in the background", cursor.closeEntered.await(10, TimeUnit.SECONDS), is(true));
        } finally {
            cursor.release.countDown();
        }
    }

    @Test
    void killDoesNotBlockWhenClientCloseHangs() throws Exception {
        Trigger trigger = Trigger.builder().id("watch").build();
        CountDownLatch closeEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        MongoClient hangingClient = (MongoClient) Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] { MongoClient.class },
            (proxy, method, args) ->
            {
                switch (method.getName()) {
                    case "close":
                        closeEntered.countDown();
                        release.await();
                        return null;
                    case "toString":
                        return "hangingMongoClient";
                    case "hashCode":
                        return System.identityHashCode(proxy);
                    case "equals":
                        return proxy == args[0];
                    default:
                        throw new UnsupportedOperationException(method.getName());
                }
            }
        );
        setField(trigger, "client", hangingClient);

        AtomicReference<Throwable> killError = new AtomicReference<>();
        CountDownLatch killDone = new CountDownLatch(1);
        Thread killer = new Thread(() ->
        {
            try {
                trigger.kill();
            } catch (Throwable t) {
                killError.set(t);
            } finally {
                killDone.countDown();
            }
        });
        killer.setDaemon(true);
        killer.start();

        try {
            assertThat("kill() must return without waiting for close()", killDone.await(10, TimeUnit.SECONDS), is(true));
            assertThat(killError.get(), nullValue());
            assertThat("close must still be attempted in the background", closeEntered.await(10, TimeUnit.SECONDS), is(true));
        } finally {
            release.countDown();
        }
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

    private static boolean awaitTrue(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(10);
        }
        return condition.getAsBoolean();
    }

    /**
     * Fake cursor blocked in {@link #hasNext()} until {@link #close()} is called,
     * mirroring a driver cursor stuck in a server round-trip.
     *
     * <p>
     * Closing the cursor only requests cancellation; like the driver, it does not
     * claim to interrupt a genuinely wedged network read.
     */
    private static final class BlockingCursor extends StubCursor {
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
    }

    /**
     * Fake cursor whose {@link #close()} blocks until released, proving that
     * {@link Trigger#kill()} dispatches closing to the background instead of
     * waiting for a synchronous teardown round trip.
     */
    private static final class HangingCloseCursor extends StubCursor {
        final CountDownLatch closeEntered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        @Override
        public void close() {
            closeEntered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public boolean hasNext() {
            throw new IllegalStateException("no data");
        }
    }

    private static abstract class StubCursor implements MongoCursor<BsonDocument> {
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
