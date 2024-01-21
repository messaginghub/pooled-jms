package org.messaginghub.pooled.jms.metrics;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.machinezoo.noexception.Exceptions;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObjectInfo;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.messaginghub.pooled.jms.pool.PooledConnection;
import org.messaginghub.pooled.jms.pool.PooledConnectionKey;

public interface JmsPoolMetrics {

    GenericKeyedObjectPool<PooledConnectionKey, PooledConnection> getConnectionsPool();

    default long getBorrowedCount() {
        return getConnectionsPool().getBorrowedCount();
    }

    default long getCreatedCount() {
        return getConnectionsPool().getCreatedCount();
    }

    default long getReturnedCount() {
        return getConnectionsPool().getReturnedCount();
    }

    default long getDestroyedByBorrowValidationCount() {
        return getConnectionsPool().getDestroyedByBorrowValidationCount();
    }

    default long getDestroyedByEvictorCount() {
        return getConnectionsPool().getDestroyedByEvictorCount();
    }

    default long getDestroyedCount() {
        return getConnectionsPool().getDestroyedCount();
    }

    default Duration getMaxBorrowWaitDuration() {
        return getConnectionsPool().getMaxBorrowWaitDuration();
    }

    default Duration getMeanActiveDuration() {
        return getConnectionsPool().getMeanActiveDuration();
    }

    default Duration getMeanBorrowWaitDuration() {
        return getConnectionsPool().getMeanBorrowWaitDuration();
    }

    default Duration getMeanIdleDuration() {
        return getConnectionsPool().getMeanIdleDuration();
    }

    default int getNumActive() {
        return getConnectionsPool().getNumActive();
    }

    default Map<String, Integer> getNumActivePerKey() {
        return getConnectionsPool().getNumActivePerKey();
    }

    default int getNumIdle() {
        return getConnectionsPool().getNumIdle();
    }

    default int getNumWaiters() {
        return getConnectionsPool().getNumWaiters();
    }

    default Map<String, Integer> getNumWaitersByKey() {
        return getConnectionsPool().getNumWaitersByKey();
    }

    @SuppressWarnings("java:S3011")
    private Map<String, Integer> getStatisticsByKey(Function<PooledConnection, Integer> func) {
        try {
            Field f = DefaultPooledObjectInfo.class.getDeclaredField("pooledObject");
            f.setAccessible(true);
            return getConnectionsPool()
                    .listAllObjects()
                    .entrySet()
                    .stream()
                    .map(e -> new SimpleEntry<String, Integer>(
                    e.getKey(),
                    e
                            .getValue()
                            .stream()
                            .map(info -> Exceptions.sneak().get(() -> f.get(info)))
                            .map(obj -> (PooledObject<PooledConnection>) obj)
                            .map(PooledObject::getObject)
                            .map(func)
                            .reduce(0, Integer::sum)))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        } catch (Exception ex) {
            return Map.of();
        }
    }

    default Map<String, Integer> getNumActiveSessionsByKey() {
        return getStatisticsByKey(PooledConnection::getNumActiveSessions);
    }

    default Map<String, Integer> getNumIdleSessionsByKey() {
        return getStatisticsByKey(PooledConnection::getNumIdleSessions);
    }

    default Map<String, Integer> getNumSessionsByKey() {
        return getStatisticsByKey(PooledConnection::getNumSessions);
    }
}
