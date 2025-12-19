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
package org.messaginghub.pooled.jms;

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.EvictionConfig;
import org.apache.commons.pool2.impl.EvictionPolicy;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.messaginghub.pooled.jms.pool.PooledConnection;
import org.messaginghub.pooled.jms.pool.PooledConnectionKey;
import org.messaginghub.pooled.jms.pool.PooledSessionKey;
import org.messaginghub.pooled.jms.util.JMSExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.QueueConnection;
import jakarta.jms.QueueConnectionFactory;
import jakarta.jms.Session;
import jakarta.jms.TopicConnection;
import jakarta.jms.TopicConnectionFactory;

/**
 * A JMS provider which pools Connection, Session and MessageProducer instances
 * so it can be used with tools like <a href="http://camel.apache.org/">Camel</a> or any other project
 * that is configured using JMS ConnectionFactory resources, connections, sessions and producers are
 * returned to a pool after use so that they can be reused later without having to undergo the cost
 * of creating them again.
 *
 * <b>NOTE:</b> while this implementation does allow the creation of a collection of active consumers,
 * it does not 'pool' consumers. Pooling makes sense for connections, sessions and producers, which
 * are expensive to create and can remain idle a minimal cost. Consumers, on the other hand, are usually
 * just created at startup and left active, handling incoming messages as they come. When a consumer is
 * complete, it is best to close it rather than return it to a pool for later reuse: this is because,
 * even if a consumer is idle, the broker may keep delivering messages to the consumer's prefetch buffer,
 * where they'll get held until the consumer is active again.
 *
 * If you are creating a collection of consumers (for example, for multi-threaded message consumption), you
 * might want to consider using a lower prefetch value for each consumer (e.g. 10 or 20), to ensure that
 * all messages don't end up going to just one of the consumers. See this FAQ entry for more detail:
 * http://activemq.apache.org/i-do-not-receive-messages-in-my-second-consumer.html
 *
 * Optionally, one may configure the pool to examine and possibly evict objects as they sit idle in the
 * pool. This is performed by a "connection check" thread, which runs asynchronously. Caution should
 * be used when configuring this optional feature. Connection check runs contend with client threads for
 * access to resources in the pool, so if they run too frequently performance issues may result. The
 * connection check thread may be configured using the {@link JmsPoolConnectionFactory#setConnectionCheckInterval(long)}
 * method.  By default the value is -1 which means no connection check thread will be run.  Set to a
 * non-negative value to configure the connection check thread to run.
 */
public class JmsPoolConnectionFactory implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory {

    private static final transient Logger LOG = LoggerFactory.getLogger(JmsPoolConnectionFactory.class);

    private static final int EXHUASTION_RECOVER_RETRY_LIMIT = 20;
    private static final long EXHAUSTION_RECOVER_INITIAL_BACKOFF = 1_000L;
    private static final long EXHAUSTION_RECOVER_BACKOFF_LIMIT = 10_000L;

    private static final long DEFAULT_TIME_BETWEEN_EVICTION_RUNS = -1;

    public static final int DEFAULT_MAX_SESSIONS_PER_CONNECTION = 500;
    public static final int DEFAULT_MAX_CONNECTIONS = 1;

    protected final AtomicBoolean stopped = new AtomicBoolean(false);

    private GenericKeyedObjectPool<PooledConnectionKey, PooledConnection> connectionsPool;

    protected Object connectionFactory;

    private int maxSessionsPerConnection = DEFAULT_MAX_SESSIONS_PER_CONNECTION;
    private int maxIdleSessionsPerConnection = DEFAULT_MAX_SESSIONS_PER_CONNECTION;
    private int connectionIdleTimeout = 30 * 1000;
    private boolean blockIfSessionPoolIsFull = true;
    private long blockIfSessionPoolIsFullTimeout = -1L;
    private boolean useAnonymousProducers = true;
    private int explicitProducerCacheSize = 0;
    private boolean useProviderJMSContext = false;

    // Temporary value used to always fetch the result of makeObject.
    private final AtomicReference<PooledConnection> mostRecentlyCreated = new AtomicReference<PooledConnection>(null);

    public void initConnectionsPool() {
        if (this.connectionsPool == null) {
            final GenericKeyedObjectPoolConfig<PooledConnection> poolConfig = new GenericKeyedObjectPoolConfig<>();
            poolConfig.setJmxEnabled(false);
            this.connectionsPool = new GenericKeyedObjectPool<PooledConnectionKey, PooledConnection>(
                new KeyedPooledObjectFactory<PooledConnectionKey, PooledConnection>() {
                    @Override
                    public PooledObject<PooledConnection> makeObject(PooledConnectionKey connectionKey) throws Exception {
                        Connection delegate = createProviderConnection(connectionKey);

                        PooledConnection connection = createPooledConnection(delegate);
                        connection.setIdleTimeout(getConnectionIdleTimeout());
                        connection.setMaxSessionsPerConnection(getMaxSessionsPerConnection());
                        connection.setMaxIdleSessionsPerConnection(
                            Math.min(getMaxIdleSessionsPerConnection(), getMaxSessionsPerConnection()));
                        connection.setBlockIfSessionPoolIsFull(isBlockIfSessionPoolIsFull());
                        if (isBlockIfSessionPoolIsFull() && getBlockIfSessionPoolIsFullTimeout() > 0) {
                            connection.setBlockIfSessionPoolIsFullTimeout(getBlockIfSessionPoolIsFullTimeout());
                        }
                        connection.setUseAnonymousProducers(isUseAnonymousProducers());
                        connection.setExplicitProducerCacheSize(getExplicitProducerCacheSize());

                        LOG.trace("Created new connection: {}", connection);

                        JmsPoolConnectionFactory.this.mostRecentlyCreated.set(connection);

                        return new DefaultPooledObject<PooledConnection>(connection);
                    }

                    @Override
                    public void destroyObject(PooledConnectionKey connectionKey, PooledObject<PooledConnection> pooledObject) throws Exception {
                        PooledConnection connection = pooledObject.getObject();
                        try {
                            LOG.trace("Destroying connection: {}", connection);
                            connection.close();
                        } catch (Exception e) {
                            LOG.warn("Close connection failed for connection: " + connection + ". This exception will be ignored.",e);
                        }
                    }

                    @Override
                    public boolean validateObject(PooledConnectionKey connectionKey, PooledObject<PooledConnection> pooledObject) {
                        PooledConnection connection = pooledObject.getObject();
                        if (connection == null || connection.idleTimeoutCheck() || connection.isClosed()) {
                            LOG.trace("Connection has expired or was closed: {} and will be destroyed", connection);
                            return false;
                        }

                        // Sanity check the Connection and if it throws IllegalStateException we assume
                        // that it is closed or has failed due to some IO error.
                        try {
                            connection.getConnection().getExceptionListener();
                        } catch (IllegalStateException jmsISE) {
                            return false;
                        } catch (Exception ambiguous) {
                            // Unsure if connection is still valid so continue as if it still is.
                        }

                        return true;
                    }

                    @Override
                    public void activateObject(PooledConnectionKey connectionKey, PooledObject<PooledConnection> pooledObject) throws Exception {
                    }

                    @Override
                    public void passivateObject(PooledConnectionKey connectionKey, PooledObject<PooledConnection> pooledObject) throws Exception {
                    }

                }, poolConfig);

            // Set max idle (not max active) since our connections always idle in the pool.
            this.connectionsPool.setMaxIdlePerKey(DEFAULT_MAX_CONNECTIONS);
            this.connectionsPool.setMinIdlePerKey(1); // Always want one connection pooled.
            this.connectionsPool.setLifo(false);
            this.connectionsPool.setBlockWhenExhausted(false);
            this.connectionsPool.setTimeBetweenEvictionRuns(Duration.ofMillis(DEFAULT_TIME_BETWEEN_EVICTION_RUNS));
            this.connectionsPool.setMinEvictableIdle(Duration.ofMillis(Long.MAX_VALUE));
            this.connectionsPool.setTestOnBorrow(true);
            this.connectionsPool.setTestWhileIdle(true);

            // Don't use the default eviction policy as it ignores our own idle timeout option.
            final EvictionPolicy<PooledConnection> policy = new EvictionPolicy<PooledConnection>() {

                @Override
                public boolean evict(EvictionConfig config, PooledObject<PooledConnection> underTest, int idleCount) {
                    return false; // We use the validation of the instance to check for idle.
                }
            };

            this.connectionsPool.setEvictionPolicy(policy);
        }
    }

    /**
     * @return the currently configured ConnectionFactory used to create the pooled Connections.
     */
    public Object getConnectionFactory() {
        return connectionFactory;
    }

    /**
     * Sets the ConnectionFactory used to create new pooled Connections.
     * <p>
     * Updates to this value do not affect Connections that were previously created and placed
     * into the pool.  In order to allocate new Connections based off this new ConnectionFactory
     * it is first necessary to {@link #clear} the pooled Connections.
     *
     * @param factory
     *      The factory to use to create pooled Connections.
     */
    public void setConnectionFactory(final Object factory) {
        if (factory instanceof ConnectionFactory) {
            this.connectionFactory = factory;
        } else {
            throw new IllegalArgumentException("connectionFactory should implement jakarta.jms.ConnectionFactory");
        }
    }

    //----- JMS Connection Creation ---------------------------------------------//

    @Override
    public QueueConnection createQueueConnection() throws JMSException {
        return (QueueConnection) createConnection();
    }

    @Override
    public QueueConnection createQueueConnection(String userName, String password) throws JMSException {
        return (QueueConnection) createConnection(userName, password);
    }

    @Override
    public TopicConnection createTopicConnection() throws JMSException {
        return (TopicConnection) createConnection();
    }

    @Override
    public TopicConnection createTopicConnection(String userName, String password) throws JMSException {
        return (TopicConnection) createConnection(userName, password);
    }

    @Override
    public Connection createConnection() throws JMSException {
        return createConnection(null, null);
    }

    @Override
    public Connection createConnection(String userName, String password) throws JMSException {
        return createJmsPoolConnection(userName, password);
    }

    //----- JMS Context Creation ---------------------------------------------//

    @Override
    public JMSContext createContext() {
        return createContext(null, null, JMSContext.AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        return createContext(null, null, sessionMode);
    }

    @Override
    public JMSContext createContext(String username, String password) {
        return createContext(username, password, JMSContext.AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(String username, String password, int sessionMode) {
        if (stopped.get()) {
            LOG.debug("JmsPoolConnectionFactory is stopped, skip create new connection.");
            return null;
        }

        if (isUseProviderJMSContext()) {
            return createProviderContext(username, password, sessionMode);
        } else {
            try {
                return newPooledConnectionContext(createJmsPoolConnection(username, password), sessionMode);
            } catch (JMSException e) {
                throw JMSExceptionSupport.createRuntimeException(e);
            }
        }
    }

    //----- Setup and Close --------------------------------------------------//

    /**
     * Starts the Connection pool.
     * <p>
     * If configured to do so this method will attempt to create an initial Connection to place
     * into the pool using the default {@link ConnectionFactory#createConnection()} from the configured
     * provider {@link ConnectionFactory}.
     */
    public void start() {
        LOG.debug("Starting the JmsPoolConnectionFactory.");
        stopped.set(false);
    }

    /**
     * Stops the pool from providing any new connections and closes all pooled Connections.
     * <p>
     * This method stops services from the JMS Connection Pool closing down any Connections in
     * the pool regardless of them being loaned out at the time.  The pool cannot be restarted
     * after a call to stop.
     */
    public void stop() {
        if (stopped.compareAndSet(false, true)) {
            LOG.debug("Stopping the JmsPoolConnectionFactory, number of connections in cache: {}",
                      connectionsPool != null ? connectionsPool.getNumActive() : 0);
            try {
                if (connectionsPool != null) {
                    connectionsPool.close();
                    connectionsPool = null;
                }
            } catch (Exception ignored) {
                LOG.trace("Caught exception on close of the Connection pool: ", ignored);
            }
        }
    }

    /**
     * Clears all connections from the pool.  Each connection that is currently in the pool is
     * closed and removed from the pool.  A new connection will be created on the next call to
     * {@link #createConnection} if the pool has not been stopped.  Care should be taken when
     * using this method as Connections that are in use by the client will be closed.
     */
    public void clear() {
        if (stopped.get()) {
            return;
        }

        getConnectionsPool().clear();
    }

    //----- Connection Pool Configuration ------------------------------------//

    /**
     * Returns the currently configured maximum idle sessions per connection which by
     * default matches the configured maximum active sessions per connection.
     *
     * @return the number if idle sessions allowed per connection before they are closed.
     *
     * @see setMaxSessionsPerConnection
     * @see setMaxIdleSessionsPerConnection
     */
    public int getMaxIdleSessionsPerConnection() {
        return maxIdleSessionsPerConnection;
    }

    /**
     * Sets the configured maximum idle sessions per connection which by default matches the
     * configured maximum active sessions per connection. This option allows the pool to be
     * configured to close sessions that are returned to the pool if the number of idle (not
     * in use Sessions) exceeds this amount which can reduce the amount of resources that are
     * allocated but not in use.
     * <p>
     * If the application in use opens and closes large amounts of sessions then leaving this
     * option at the default means that there is a higher chance that an idle session will be
     * available in the pool without the need to create a new instance however this does allow
     * for more idle resources to exist so in cases where turnover is low with only occasional
     * bursts in workloads it can be advantageous to lower this value to allow sessions to be
     * fully closed on return to the pool if there are already enough idle sessions to exceed
     * this amount.
     * <p>
     * If the max idle sessions per connection is configured larger than the max sessions value
     * it will be truncated to the max sessions value to conform to the total limit on how many
     * sessions can exists at any given time on a per connection basis.
     *
     * @param maxIdleSessionsPerConnection
     *    the number of idle sessions allowed per connection before they are closed.
     *
     * @see setMaxSessionsPerConnection
     */
    public void setMaxIdleSessionsPerConnection(int maxIdleSessionsPerConnection) {
        this.maxIdleSessionsPerConnection = maxIdleSessionsPerConnection;
    }

    /**
     * Returns the currently configured maximum number of sessions a pooled Connection will
     * create before it either blocks or throws an exception when a new session is requested,
     * depending on configuration.
     *
     * @return the number of session instances that can be taken from a pooled connection.
     *
     * @see setMaxSessionsPerConnection
     * @see setMaxIdleSessionsPerConnection
     */
    public int getMaxSessionsPerConnection() {
        return maxSessionsPerConnection;
    }

    /**
     * Sets the maximum number of pooled sessions allowed per connection.
     * <p>
     * A Connection that is created from this JMS Connection pool can limit the number
     * of Sessions that are created and loaned out.  When a limit is in place the client
     * application must be prepared to respond to failures or hangs of the various
     * {@link Connection#createSession()} methods.
     * <p>
     * Because Connections can be borrowed and returned at will the available Sessions for
     * a Connection in the pool can change dynamically so even on fresh checkout from this
     * pool a Connection may not have any available Session instances to loan out if a limit
     * is configured.
     *
     * @param maxSessionsPerConnection
     *      The maximum number of pooled sessions per connection in the pool.
     */
    public void setMaxSessionsPerConnection(int maxSessionsPerConnection) {
        this.maxSessionsPerConnection = maxSessionsPerConnection;
    }

    /**
     * Controls the behavior of the internal session pool. By default the call to
     * {@link Connection#createSession()} will block if the session pool is full.  If the
     * block options is set to false, it will change the default behavior and instead the
     * call to create a {@link Session} will throw a JMSException.
     * <p>
     * The size of the session pool is controlled by the {@link #getMaxSessionsPerConnection()}
     * configuration property.
     *
     * @param block
     * 		if true, the call to {@link Connection#createSession()} blocks if the session pool is full
     *      until a session is available.  defaults to true.
     */
    public void setBlockIfSessionPoolIsFull(boolean block) {
        this.blockIfSessionPoolIsFull = block;
    }

    /**
     * Returns whether a pooled Connection will enter a blocked state or will throw an Exception
     * once the maximum number of sessions has been borrowed from the the Session Pool.
     *
     * @return true if the pooled Connection createSession method will block when the limit is hit.
     *
     * @see #setBlockIfSessionPoolIsFull(boolean)
     */
    public boolean isBlockIfSessionPoolIsFull() {
        return this.blockIfSessionPoolIsFull;
    }

    /**
     * Returns the maximum number to pooled Connections that this factory will allow before it
     * begins to return existing connections from the pool on calls to ({@link #createConnection}.
     *
     * @return the maxConnections that will be created for this pool.
     */
    public int getMaxConnections() {
        return getConnectionsPool().getMaxIdlePerKey();
    }

    /**
     * Sets the maximum number of pooled Connections (defaults to one).  Each call to
     * {@link #createConnection} will result in a new Connection being created up to the max
     * connections value, once the maximum Connections have been created Connections are served
     * in a last in first out ordering.
     *
     * @param maxConnections
     * 		the maximum Connections to pool for a given user / password combination.
     */
    public void setMaxConnections(int maxConnections) {
        getConnectionsPool().setMaxIdlePerKey(maxConnections);
        getConnectionsPool().setMaxTotalPerKey(maxConnections);
    }

    /**
     * Gets the idle timeout value applied to Connection's that are created by this pool but are
     * not currently in use.
     *
     * @return the connection idle timeout value in (milliseconds).
     */
    public int getConnectionIdleTimeout() {
        return connectionIdleTimeout;
    }

    /**
     * Sets the idle timeout value for Connection's that are created by this pool but not in use in
     * Milliseconds (defaults to 30 seconds).
     * <p>
     * For a Connection that is in the pool but has no current users the idle timeout determines how
     * long the Connection can live before it is eligible for removal from the pool.  Normally the
     * connections are tested when an attempt to check one out occurs so a Connection instance can sit
     * in the pool much longer than its idle timeout if connections are used infrequently.  To evict idle
     * connections in a more timely manner the {@link #setConnectionCheckInterval(long)} can be configured
     * to a non-zero value and the pool will actively check for idle connections that have exceeded their
     * idle timeout value.
     *
     * @param connectionIdleTimeout
     *      The maximum time a pooled Connection can sit unused before it is eligible for removal.
     *
     * @see #setConnectionCheckInterval(long)
     */
    public void setConnectionIdleTimeout(int connectionIdleTimeout) {
        this.connectionIdleTimeout = connectionIdleTimeout;
    }

    /**
     * Should Sessions use one anonymous producer for all producer requests or should a new
     * MessageProducer be created for each request to create a producer object, default is true.
     * <p>
     * When enabled the session only needs to allocate one MessageProducer for all requests and
     * the MessageProducer#send(destination, message) method can be used.  Normally this is the
     * right thing to do however it does result in the Broker not showing the producers per
     * destination.
     *
     * @return true if a pooled Session will use only a single anonymous message producer instance.
     */
    public boolean isUseAnonymousProducers() {
        return this.useAnonymousProducers;
    }

    /**
     * Sets whether a pooled Session uses only one anonymous MessageProducer instance or creates
     * a new MessageProducer for each call the create a MessageProducer.
     *
     * @param value
     *      Boolean value that configures whether anonymous producers are used.
     */
    public void setUseAnonymousProducers(boolean value) {
        this.useAnonymousProducers = value;
    }

    /**
     * Returns the currently configured producer cache size that will be used in a pooled
     * Session when the pooled Session is not configured to use a single anonymous producer.
     *
     * @return the current explicit producer cache size.
     */
    public int getExplicitProducerCacheSize() {
        return this.explicitProducerCacheSize;
    }

    /**
     * Sets whether a pooled Session uses a cache for MessageProducer instances that are
     * created against an explicit destination instead of creating new MessageProducer on each
     * call to {@linkplain Session#createProducer(jakarta.jms.Destination)}.
     * <p>
     * When caching explicit producers the cache will hold up to the configured number of producers
     * and if more producers are created than the configured cache size the oldest or lest recently
     * used producers are evicted from the cache and will be closed when all references to that
     * producer are explicitly closed or when the pooled session instance is closed.  By default this
     * value is set to zero and no caching is done for explicit producers created by the pooled session.
     * <p>
     * This caching would only be done when the {@link #setUseAnonymousProducers(boolean)} configuration
     * option is disabled.
     *
     * @param cacheSize
     * 		The number of explicit producers to cache in the pooled Session
     */
    public void setExplicitProducerCacheSize(int cacheSize) {
        this.explicitProducerCacheSize = cacheSize;
    }

    /**
     * Sets the number of milliseconds to sleep between runs of the Connection check thread.
     * When non-positive, no connection check thread will be run, and Connections will only be
     * checked on borrow to determine if they are still valid and can continue to be used or should
     * be closed and or evicted from the pool.
     * <p>
     * By default this value is set to -1 and a connection check thread is not started.
     *
     * @param connectionCheckInterval
     *      The time to wait between runs of the Connection check thread.
     *
     * @see #setConnectionIdleTimeout(int)
     */
    public void setConnectionCheckInterval(long connectionCheckInterval) {
        getConnectionsPool().setTimeBetweenEvictionRuns(Duration.ofMillis(connectionCheckInterval));
    }

    /**
     * @return the number of milliseconds to sleep between runs of the connection check thread.
     */
    public long getConnectionCheckInterval() {
        return getConnectionsPool().getDurationBetweenEvictionRuns().toMillis();
    }

    /**
     * @return the number of Connections currently in the Pool
     */
    public int getNumConnections() {
        return getConnectionsPool().getNumIdle();
    }

    /**
     * Returns the timeout to use for blocking creating new sessions
     *
     * @return true if the pooled Connection createSession method will block when the limit is hit.
     *
     * @see #setBlockIfSessionPoolIsFull(boolean)
     */
    public long getBlockIfSessionPoolIsFullTimeout() {
        return blockIfSessionPoolIsFullTimeout;
    }

    /**
     * Controls the behavior of the internal {@link Session} pool. By default the call to
     * Connection.getSession() will block if the {@link Session} pool is full.  This setting
     * will affect how long it blocks and throws an exception after the timeout.
     * <p>
     * The size of the session pool is controlled by the {@link #setMaxSessionsPerConnection(int)}
     * value that has been configured.  Whether or not the call to create session blocks is controlled
     * by the {@link #setBlockIfSessionPoolIsFull(boolean)} property.
     * <p>
     * By default the timeout defaults to -1 and a blocked call to create a Session will
     * wait indefinitely for a new {@link Session}
     *
     * @param blockIfSessionPoolIsFullTimeout
     * 		if blockIfSessionPoolIsFullTimeout is true then use this setting
     *      to configure how long to block before an error is thrown.
     *
     * @see #setMaxSessionsPerConnection(int)
     */
    public void setBlockIfSessionPoolIsFullTimeout(long blockIfSessionPoolIsFullTimeout) {
        this.blockIfSessionPoolIsFullTimeout = blockIfSessionPoolIsFullTimeout;
    }

    /**
     * @return the true if the pool is using the provider's JMSContext instead of a pooled version.
     */
    public boolean isUseProviderJMSContext() {
        return useProviderJMSContext;
    }

    /**
     * Controls the behavior of the {@link JmsPoolConnectionFactory#createContext} methods.
     * <p>
     * By default this value is set to false and the JMS Pool will use n pooled version of
     * a JMSContext to wrap Connections from the pool.  These pooled JMSContext objects have certain
     * limitations which may not be desirable in some cases.  To use the JMSContext implementation
     * from the underlying JMS provider this option can be set to true however in that case no
     * pooling will be applied to the JMSContext's create or their underlying connections.
     *
     * @param useProviderJMSContext
     * 		Boolean value indicating whether the pool should include JMSContext in the pooling.
     */
    public void setUseProviderJMSContext(boolean useProviderJMSContext) {
        this.useProviderJMSContext = useProviderJMSContext;
    }

    //----- Internal implementation ------------------------------------------//

    /**
     * Gets the Pool of ConnectionPool instances which are keyed by different ConnectionKeys.
     *
     * @return this factories pool of ConnectionPool instances.
     */
    protected GenericKeyedObjectPool<PooledConnectionKey, PooledConnection> getConnectionsPool() {
        initConnectionsPool();
        return this.connectionsPool;
    }

    /**
     * Delegate that creates each instance of an ConnectionPool object.  Subclasses can override
     * this method to customize the type of connection pool returned.
     *
     * @param connection
     * 		The connection that is being added into the pool.
     *
     * @return instance of a new ConnectionPool.
     */
    protected PooledConnection createPooledConnection(Connection connection) {
        return new PooledConnection(connection);
    }

    /**
     * Allows subclasses to create an appropriate JmsPoolConnection wrapper for the newly
     * create connection such as one that provides support for XA Transactions.
     *
     * @param connection
     * 		The {@link PooledConnection} to wrap.
     *
     * @return a new {@link JmsPoolConnection} that wraps the given {@link PooledConnection}
     */
    protected JmsPoolConnection newPooledConnectionWrapper(PooledConnection connection) {
        return new JmsPoolConnection(connection);
    }

    /**
     * Allows subclasses to create an appropriate JmsPoolJMSContext wrapper for the newly
     * create JMSContext such as one that provides support for XA Transactions.
     *
     * @param connection
     * 		The {@link JmsPoolConnection} to use in the JMSContext wrapper.
     * @param sessionMode
     * 		The JMS Session acknowledgement mode to use in the {@link JMSContext}
     *
     * @return a new {@link JmsPoolJMSContext} that wraps the given {@link JmsPoolConnection}
     */
    protected JmsPoolJMSContext newPooledConnectionContext(JmsPoolConnection connection, int sessionMode) {
        return new JmsPoolJMSContext(connection, sessionMode);
    }

    /**
     * Given a {@link PooledConnectionKey} create a JMS {@link Connection} using the
     * configuration from the key and the assigned JMS {@link ConnectionFactory} instance.
     *
     * @param key
     * 		The {@link PooledSessionKey} to use as configuration for the new JMS Connection.
     *
     * @return a new JMS Connection created using the configured JMS ConnectionFactory.
     *
     * @throws JMSException if an error occurs while creating the new JMS Connection.
     */
    protected Connection createProviderConnection(PooledConnectionKey key) throws JMSException {
        if (connectionFactory instanceof ConnectionFactory) {
            if (key.getUserName() == null && key.getPassword() == null) {
                return ((ConnectionFactory) connectionFactory).createConnection();
            } else {
                return ((ConnectionFactory) connectionFactory).createConnection(key.getUserName(), key.getPassword());
            }
        } else {
            throw new IllegalStateException("connectionFactory should implement jakarta.jms.ConnectionFactory");
        }
    }

    /**
     * Create a new {@link JMSContext} using the provided credentials and Session mode
     *
     * @param username
     * 		The user name to use when creating the context.
     * @param password
     * 		The password to use when creating the context.
     * @param sessionMode
     * 		The session mode to use when creating the context.
     *
     * @return a new JMSContext created using the given configuration data..
     *
     * @throws JMSRuntimeException if an error occurs while creating the new JMS Context.
     */
    protected JMSContext createProviderContext(String username, String password, int sessionMode) {
        if (connectionFactory instanceof ConnectionFactory) {
            if (username == null && password == null) {
                return ((ConnectionFactory) connectionFactory).createContext(sessionMode);
            } else {
                return ((ConnectionFactory) connectionFactory).createContext(username, password, sessionMode);
            }
        } else {
            throw new jakarta.jms.IllegalStateRuntimeException("connectionFactory should implement jakarta.jms.ConnectionFactory");
        }
    }

    private synchronized JmsPoolConnection createJmsPoolConnection(String userName, String password) throws JMSException {
        if (stopped.get()) {
            LOG.debug("JmsPoolConnectionFactory is stopped, skip create new connection.");
            return null;
        }

        if (connectionFactory == null) {
            throw new IllegalStateException("No ConnectionFactory instance has been configured");
        }

        PooledConnection connection = null;
        PooledConnectionKey key = new PooledConnectionKey(userName, password);

        // This will either return an existing non-expired ConnectionPool or it
        // will create a new one to meet the demand in most cases but can be raced
        // and result in no addition which will then fall into the borrow loop below
        // which attempts to take a connection from the active set.
        if (getConnectionsPool().getNumIdle(key) < getMaxConnections()) {
            try {
                connectionsPool.addObject(key);
                connection = mostRecentlyCreated.getAndSet(null);
                if (connection != null) {
                    connection.incrementReferenceCount();
                }
            } catch (Exception e) {
                throw JMSExceptionSupport.create("Error while attempting to add new Connection to the pool", e);
            }
        }

        if (connection == null) {
            try {
                int exhaustedPoolRecoveryAttempts = 0;
                long exhaustedPoolRecoveryBackoff = EXHAUSTION_RECOVER_INITIAL_BACKOFF;

                // We can race against other threads returning the connection when there is an
                // expiration or idle timeout.  We keep pulling out ConnectionPool instances until
                // we win and get a non-closed instance and then increment the reference count
                // under lock to prevent another thread from triggering an expiration check and
                // pulling the rug out from under us.
                while (connection == null) {
                    try {
                        connection = connectionsPool.borrowObject(key);
                    } catch (NoSuchElementException nse) {
                        if (exhaustedPoolRecoveryAttempts++ < EXHUASTION_RECOVER_RETRY_LIMIT) {
                            LOG.trace("Recover attempt {} from exhausted pool by refilling pool key and creating new Connection", exhaustedPoolRecoveryAttempts);
                            if (exhaustedPoolRecoveryAttempts > 1) {
                                LockSupport.parkNanos(exhaustedPoolRecoveryBackoff);
                                exhaustedPoolRecoveryBackoff = Math.min(EXHAUSTION_RECOVER_BACKOFF_LIMIT,
                                                                        exhaustedPoolRecoveryBackoff + exhaustedPoolRecoveryBackoff);
                            } else {
                                Thread.yield();
                            }

                            connectionsPool.addObject(key);
                            continue;
                        } else {
                            throw JMSExceptionSupport.createResourceAllocationException(nse);
                        }
                    }
                    synchronized (connection) {
                        if (connection.isClosed()) {
                            // Return the bad one to the pool and let if get destroyed as normal.
                            connectionsPool.returnObject(key, connection);
                            connection = null;
                        } else {
                            connection.incrementReferenceCount();
                        }
                    }
                }
            } catch (Exception e) {
                throw JMSExceptionSupport.create("Error while attempting to retrieve a connection from the pool", e);
            }

            try {
                connectionsPool.returnObject(key, connection);
            } catch (Exception e) {
                throw JMSExceptionSupport.create("Error when returning connection to the pool", e);
            }
        }

        return newPooledConnectionWrapper(connection);
    }

    //----- JNDI Operations --------------------------------------------------//

    /**
     * Called by any superclass that implements a JNDI Referenceable or similar that needs to collect
     * the properties of this class for storage etc.
     *
     * This method should be updated any time there is a new property added.
     *
     * @param props
     *        a properties object that should be filled in with this objects property values.
     */
    protected void populateProperties(Properties props) {
        props.setProperty("maxSessionsPerConnection", Integer.toString(getMaxSessionsPerConnection()));
        props.setProperty("maxConnections", Integer.toString(getMaxConnections()));
        props.setProperty("connectionIdleTimeout", Integer.toString(getConnectionIdleTimeout()));
        props.setProperty("connectionCheckInterval", Long.toString(getConnectionCheckInterval()));
        props.setProperty("useAnonymousProducers", Boolean.toString(isUseAnonymousProducers()));
        props.setProperty("blockIfSessionPoolIsFull", Boolean.toString(isBlockIfSessionPoolIsFull()));
        props.setProperty("blockIfSessionPoolIsFullTimeout", Long.toString(getBlockIfSessionPoolIsFullTimeout()));
        props.setProperty("useProviderJMSContext", Boolean.toString(isUseProviderJMSContext()));
    }
}
