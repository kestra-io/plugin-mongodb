package io.kestra.plugin.mongodb;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Set;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;

import lombok.experimental.SuperBuilder;

/**
 * Throws the given error from every single-statement write (update, replace, delete), to test failures that need a
 * real failover. Overriding the task instead would register a plugin.
 */
@SuperBuilder
class FailingWriteConnection extends MongoDbConnection {
    private static final Set<String> WRITES = Set.of("updateOne", "updateMany", "replaceOne", "deleteOne", "deleteMany");

    private final MongoException failure;

    @Override
    MongoClient client(RunContext runContext, CommandListener listener) throws IllegalVariableEvaluationException {
        return failWrites(super.client(runContext, listener), MongoClient.class, failure);
    }

    // proxies the client, and the databases and collections it returns, down to the write calls
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T> T failWrites(T target, Class<T> type, MongoException failure) {
        return type.cast(Proxy.newProxyInstance(
            type.getClassLoader(),
            new Class<?>[] {type},
            (proxy, method, args) ->
            {
                if (WRITES.contains(method.getName())) {
                    throw failure;
                }

                Object result;
                try {
                    result = method.invoke(target, args);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }

                if (result instanceof MongoDatabase database) {
                    return failWrites(database, MongoDatabase.class, failure);
                }
                if (result instanceof MongoCollection collection) {
                    return failWrites(collection, (Class) MongoCollection.class, failure);
                }
                return result;
            }
        ));
    }
}
