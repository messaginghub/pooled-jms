/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.messaginghub.pooled.jms.pool;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.messaginghub.pooled.jms.JmsPoolSession;
import org.messaginghub.pooled.jms.JmsPoolSessionEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds a real JMS connection along with the session pools associated with it.
 * <p>
 * Instances of this class are shared amongst one or more PooledConnection object and must
 * track the session objects that are loaned out for cleanup on close as well as ensuring
 * that the temporary destinations of the managed Connection are purged when all references
 * to this ConnectionPool are released.
 */
public class PooledConnection implements ExceptionListener {

    private static final transient Logger LOG = LoggerFactory.getLogger(PooledConnection.class);

    protected Connection connection;
    private int referenceCount;
    private long lastUsed = System.currentTimeMillis();
    private boolean hasExpired;
    private int idleTimeout = 30 * 1000;
    private boolean useAnonymousProducers = true;
    private int explicitProducerCacheSize = 0;
    private int jmsMajorVersion = 1;
    private int jmsMinorVersion = 1;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final GenericKeyedObjectPool<PooledSessionKey, PooledSessionHolder> sessionPool;
    private final List<JmsPoolSession> loanedSessions = new CopyOnWriteArrayList<JmsPoolSession>();
    private ExceptionListener parentExceptionListener;

    public PooledConnection(Connection connection) {
        final GenericKeyedObjectPoolConfig<PooledSessionHolder> poolConfig = new GenericKeyedObjectPoolConfig<>();
        poolConfig.setJmxEnabled(false);
        this.connection = wrap(connection);
        try {
            this.connection.setExceptionListener(this);
        } catch (JMSException ex) {
            LOG.warn("Could not set exception listener on create of ConnectionPool");
        }

        // Check if JMS API on the classpath supports JMS 2+
        try {
            Connection.class.getMethod("createSession", int.class);
            // Determine the version range of this JMS 2+ client
            try {
                jmsMajorVersion = connection.getMetaData().getJMSMajorVersion();
                jmsMinorVersion = connection.getMetaData().getJMSMajorVersion();
            } catch (JMSException ex) {
                LOG.debug("Error while fetching JMS API version from provider, defaulting to v1.1");
                jmsMajorVersion = 1;
                jmsMinorVersion = 1;
            }
        } catch (NoSuchMethodException nsme) {
            LOG.trace("JMS API on the classpath is not JMS 2.0+ defaulting to v1.1 internally");
        }

        // Create our internal Pool of session instances.
        this.sessionPool = new GenericKeyedObjectPool<PooledSessionKey, PooledSessionHolder>(
            new KeyedPooledObjectFactory<PooledSessionKey, PooledSessionHolder>() {
                @Override
                public PooledObject<PooledSessionHolder> makeObject(PooledSessionKey sessionKey) throws Exception {
                    return new DefaultPooledObject<PooledSessionHolder>(
                        new PooledSessionHolder(PooledConnection.this, makeSession(sessionKey), useAnonymousProducers, explicitProducerCacheSize));
                }

                @Override
                public void destroyObject(PooledSessionKey sessionKey, PooledObject<PooledSessionHolder> pooledObject) throws Exception {
                    pooledObject.getObject().close();
                }

                @Override
                public boolean validateObject(PooledSessionKey sessionKey, PooledObject<PooledSessionHolder> pooledObject) {
                    return true;
                }

                @Override
                public void activateObject(PooledSessionKey sessionKey, PooledObject<PooledSessionHolder> pooledObject) throws Exception {
                }

                @Override
                public void passivateObject(PooledSessionKey sessionKey, PooledObject<PooledSessionHolder> pooledObject) throws Exception {
                }
            }, poolConfig
        );
    }

    // useful when external failure needs to force expiration
    public void setHasExpired(boolean val) {
        hasExpired = val;
    }

    protected Session makeSession(PooledSessionKey key) throws JMSException {
        return connection.createSession(key.isTransacted(), key.getAckMode());
    }

    protected Connection wrap(Connection connection) {
        return connection;
    }

    protected void unWrap(Connection connection) {
    }

    public void start() throws JMSException {
        if (started.compareAndSet(false, true)) {
            try {
                connection.start();
            } catch (Throwable error) {
                started.set(false);
                close();
                throw error;
            }
        }
    }

    public synchronized Connection getConnection() {
        return connection;
    }

    public Session createSession(boolean transacted, int ackMode) throws JMSException {
        PooledSessionKey key = new PooledSessionKey(transacted, ackMode);
        JmsPoolSession session;
        try {
            session = new JmsPoolSession(key, sessionPool.borrowObject(key), sessionPool, key.isTransacted());
            session.addSessionEventListener(new JmsPoolSessionEventListener() {

                @Override
                public void onTemporaryTopicCreate(TemporaryTopic tempTopic) {
                }

                @Override
                public void onTemporaryQueueCreate(TemporaryQueue tempQueue) {
                }

                @Override
                public void onSessionClosed(JmsPoolSession session) {
                    PooledConnection.this.loanedSessions.remove(session);
                }
            });
            this.loanedSessions.add(session);
        } catch (Exception e) {
            IllegalStateException illegalStateException = new IllegalStateException(e.toString());
            illegalStateException.initCause(e);
            throw illegalStateException;
        }
        return session;
    }

    public synchronized void close() {
        if (connection != null) {
            try {
                sessionPool.close();
            } catch (Exception e) {
            } finally {
                try {
                    connection.close();
                } catch (Exception e) {
                } finally {
                    connection = null;
                }
            }
        }
    }

    public synchronized void incrementReferenceCount() {
        referenceCount++;
        lastUsed = System.currentTimeMillis();
    }

    public synchronized void decrementReferenceCount() {
        referenceCount--;
        lastUsed = System.currentTimeMillis();
        if (referenceCount == 0) {
            // Loaned sessions are those that are active in the sessionPool and
            // have not been closed by the client before closing the connection.
            // These need to be closed so that all session's reflect the fact
            // that the parent Connection is closed.
            for (JmsPoolSession session : this.loanedSessions) {
                try {
                    session.close();
                } catch (Exception e) {
                }
            }
            this.loanedSessions.clear();

            unWrap(getConnection());

            expiredCheck();
        }
    }

    /**
     * Determines if this Connection has expired.
     * <p>
     * A ConnectionPool is considered expired when all references to it are released AND either
     * the configured idleTimeout has elapsed OR the configured expiryTimeout has elapsed.
     * Once a ConnectionPool is determined to have expired its underlying Connection is closed.
     *
     * @return true if this connection has expired.
     */
    public synchronized boolean expiredCheck() {

        boolean expired = false;

        if (connection == null) {
            return true;
        }

        if (hasExpired) {
            if (referenceCount == 0) {
                close();
                expired = true;
            }
        }

        // Only set hasExpired here is no references, as a Connection with references is by
        // definition not idle at this time.
        if (referenceCount == 0 && idleTimeout > 0 && (lastUsed + idleTimeout) - System.currentTimeMillis() < 0) {
            hasExpired = true;
            close();
            expired = true;
        }

        return expired;
    }

    public int getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(int idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public int getMaxSessionsPerConnection() {
        return this.sessionPool.getMaxTotalPerKey();
    }

    public void setMaxSessionsPerConnection(int maActiveSessionsPerConnection) {
        this.sessionPool.setMaxTotalPerKey(maActiveSessionsPerConnection);
    }

    public boolean isUseAnonymousProducers() {
        return this.useAnonymousProducers;
    }

    public void setUseAnonymousProducers(boolean value) {
        this.useAnonymousProducers = value;
    }

    public int getExplicitProducerCacheSize() {
        return this.explicitProducerCacheSize;
    }

    public void setExplicitProducerCacheSize(int cacheSize) {
        this.explicitProducerCacheSize = cacheSize;
    }

    /**
     * @return the total number of Pooled session including idle sessions that are not
     *          currently loaned out to any client.
     */
    public int getNumSessions() {
        return this.sessionPool.getNumIdle() + this.sessionPool.getNumActive();
    }

    /**
     * @return the total number of Sessions that are in the Session pool but not loaned out.
     */
    public int getNumIdleSessions() {
        return this.sessionPool.getNumIdle();
    }

    /**
     * @return the total number of Session's that have been loaned to PooledConnection instances.
     */
    public int getNumActiveSessions() {
        return this.sessionPool.getNumActive();
    }

    /**
     * Configure whether the createSession method should block when there are no more idle sessions and the
     * pool already contains the maximum number of active sessions.  If false the create method will fail
     * and throw an exception.
     *
     * @param block
     * 		Indicates whether blocking should be used to wait for more space to create a session.
     */
    public void setBlockIfSessionPoolIsFull(boolean block) {
        this.sessionPool.setBlockWhenExhausted(block);
    }

    public boolean isBlockIfSessionPoolIsFull() {
        return this.sessionPool.getBlockWhenExhausted();
    }

    /**
     * Returns the timeout to use for blocking creating new sessions
     *
     * @return true if the pooled Connection createSession method will block when the limit is hit.
     * @see #setBlockIfSessionPoolIsFull(boolean)
     */
    public long getBlockIfSessionPoolIsFullTimeout() {
        return this.sessionPool.getMaxWaitMillis();
    }

    /**
     * Controls the behavior of the internal session pool. By default the call to
     * Connection.getSession() will block if the session pool is full.  This setting
     * will affect how long it blocks and throws an exception after the timeout.
     *
     * The size of the session pool is controlled by the @see #maximumActive
     * property.
     *
     * Whether or not the call to create session blocks is controlled by the @see #blockIfSessionPoolIsFull
     * property
     *
     * @param blockIfSessionPoolIsFullTimeout - if blockIfSessionPoolIsFullTimeout is true,
     *                                        then use this setting to configure how long to block before retry
     */
    public void setBlockIfSessionPoolIsFullTimeout(long blockIfSessionPoolIsFullTimeout) {
        this.sessionPool.setMaxWaitMillis(blockIfSessionPoolIsFullTimeout);
    }

    /**
     * Checks for JMS version support in the underlying JMS Connection this pooled connection
     * wrapper encapsulates.
     *
     * @param requiredMajor
     * 		The JMS Major version required for a feature to be supported.
     * @param requiredMinor
     * 		The JMS Minor version required for a feature to be supported.
     *
     * @return true if the Connection supports the version range given.
     */
    public boolean isJMSVersionSupported(int requiredMajor, int requiredMinor) {
        if (jmsMajorVersion >= requiredMajor && jmsMinorVersion >= requiredMinor) {
            return true;
        }

        return false;
    }

    public ExceptionListener getParentExceptionListener() {
        return parentExceptionListener;
    }

    public void setParentExceptionListener(ExceptionListener parentExceptionListener) {
        this.parentExceptionListener = parentExceptionListener;
    }

    @Override
    public void onException(JMSException exception) {
        // Closes the underlying connection and removes it from the pool
        close();

        if (parentExceptionListener != null) {
            parentExceptionListener.onException(exception);
        }
    }

    @Override
    public String toString() {
        return "ConnectionPool[" + connection + "]";
    }

    public void checkClientJMSVersionSupport(int requiredMajor, int requiredMinor) throws JMSException {
        checkClientJMSVersionSupport(requiredMajor, requiredMinor, false);
    }

    public void checkClientJMSVersionSupport(int requiredMajor, int requiredMinor, boolean runtimeEx) throws JMSException {
        if (jmsMajorVersion >= requiredMajor && jmsMinorVersion >= requiredMinor) {
            return;
        }

        String message = "JMS v" + requiredMajor + "." + requiredMinor + " client feature requested, " +
                         "configured client supports JMS v" + jmsMajorVersion + "." + jmsMinorVersion;

        if (runtimeEx) {
            throw new JMSRuntimeException(message);
        } else {
            throw new JMSException(message);
        }
    }
}
