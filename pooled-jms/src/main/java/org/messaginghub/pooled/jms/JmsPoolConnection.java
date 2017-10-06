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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;

import org.messaginghub.pooled.jms.pool.PooledConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a proxy {@link Connection} which is-a {@link TopicConnection} and
 * {@link QueueConnection} which is pooled and on {@link #close()} will return
 * its reference to the ConnectionPool backing it.
 *
 * <b>NOTE</b> this implementation is only intended for use when sending
 * messages. It does not deal with pooling of consumers.
 */
public class JmsPoolConnection implements TopicConnection, QueueConnection, JmsPoolSessionEventListener, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(JmsPoolConnection.class);

    protected PooledConnection connection;

    private final AtomicBoolean closed = new AtomicBoolean();

    private final List<TemporaryQueue> connTempQueues = new CopyOnWriteArrayList<TemporaryQueue>();
    private final List<TemporaryTopic> connTempTopics = new CopyOnWriteArrayList<TemporaryTopic>();
    private final List<JmsPoolSession> loanedSessions = new CopyOnWriteArrayList<JmsPoolSession>();

    /**
     * Creates a new PooledConnection instance that uses the given ConnectionPool to create
     * and manage its resources.  The ConnectionPool instance can be shared amongst many
     * PooledConnection instances.
     *
     * @param pool
     *      The connection and pool manager backing this proxy connection object.
     */
    public JmsPoolConnection(PooledConnection pool) {
        this.connection = pool;
    }

    @Override
    public void close() throws JMSException {
        if (closed.compareAndSet(false, true)) {
            this.cleanupConnectionTemporaryDestinations();
            this.cleanupAllLoanedSessions();
            if (this.connection != null) {
                this.connection.decrementReferenceCount();
                this.connection = null;
            }
        }
    }

    @Override
    public void start() throws JMSException {
        checkClosed();
        connection.start();
    }

    @Override
    public void stop() throws JMSException {
        checkClosed();
    }

    @Override
    public ConnectionMetaData getMetaData() throws JMSException {
        return getConnection().getMetaData();
    }

    @Override
    public ExceptionListener getExceptionListener() throws JMSException {
        checkClosed();
        return connection.getParentExceptionListener();
    }

    @Override
    public void setExceptionListener(ExceptionListener exceptionListener) throws JMSException {
        checkClosed();
        connection.setParentExceptionListener(exceptionListener);
    }

    @Override
    public String getClientID() throws JMSException {
        return getConnection().getClientID();
    }

    @Override
    public void setClientID(String clientID) throws JMSException {
        // ignore repeated calls to setClientID() with the same client id
        // this could happen when a JMS component such as Spring that uses a
        // Pooled JMS ConnectionFactory when it shuts down and reinitializes.
        if (this.getConnection().getClientID() == null || !this.getClientID().equals(clientID)) {
            getConnection().setClientID(clientID);
        }
    }

    //----- ConnectionConsumer factory methods -------------------------------//

    @Override
    public ConnectionConsumer createConnectionConsumer(Destination destination, String selector, ServerSessionPool serverSessionPool, int maxMessages) throws JMSException {
        return getConnection().createConnectionConsumer(destination, selector, serverSessionPool, maxMessages);
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Topic topic, String s, ServerSessionPool serverSessionPool, int maxMessages) throws JMSException {
        return getConnection().createConnectionConsumer(topic, s, serverSessionPool, maxMessages);
    }

    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String selector, String s1, ServerSessionPool serverSessionPool, int i) throws JMSException {
        return getConnection().createDurableConnectionConsumer(topic, selector, s1, serverSessionPool, i);
    }

    @Override
    public ConnectionConsumer createSharedDurableConnectionConsumer(Topic topic, String subscriptionName, String selector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkClosed();
        connection.checkClientJMSVersionSupport(2, 0);
        return getConnection().createSharedDurableConnectionConsumer(topic, subscriptionName, selector, sessionPool, maxMessages);
    }

    @Override
    public ConnectionConsumer createSharedConnectionConsumer(Topic topic, String subscriptionName, String selector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkClosed();
        connection.checkClientJMSVersionSupport(2, 0);
        return getConnection().createSharedConnectionConsumer(topic, subscriptionName, selector, sessionPool, maxMessages);
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Queue queue, String selector, ServerSessionPool serverSessionPool, int maxMessages) throws JMSException {
        return getConnection().createConnectionConsumer(queue, selector, serverSessionPool, maxMessages);
    }

    //----- Session factory methods ------------------------------------------//

    @Override
    public QueueSession createQueueSession(boolean transacted, int ackMode) throws JMSException {
        return (QueueSession) createSession(transacted, ackMode);
    }

    @Override
    public TopicSession createTopicSession(boolean transacted, int ackMode) throws JMSException {
        return (TopicSession) createSession(transacted, ackMode);
    }

    @Override
    public Session createSession() throws JMSException {
        return createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    public Session createSession(int sessionMode) throws JMSException {
        return createSession(sessionMode == Session.SESSION_TRANSACTED, sessionMode);
    }

    @Override
    public Session createSession(boolean transacted, int ackMode) throws JMSException {
        checkClosed();

        JmsPoolSession result = (JmsPoolSession) connection.createSession(transacted, ackMode);

        // Store the session so we can close the sessions that this Pooled JMS Connection
        // created in order to ensure that consumers etc are closed per the JMS contract.
        loanedSessions.add(result);

        // Add a event listener to the session that notifies us when the session
        // creates / destroys temporary destinations and closes etc.
        result.addSessionEventListener(this);
        return result;
    }

    //----- Pooled JMS Session specific methods ------------------------------//

    @Override
    public void onTemporaryQueueCreate(TemporaryQueue tempQueue) {
        connTempQueues.add(tempQueue);
    }

    @Override
    public void onTemporaryTopicCreate(TemporaryTopic tempTopic) {
        connTempTopics.add(tempTopic);
    }

    @Override
    public void onSessionClosed(JmsPoolSession session) {
        this.loanedSessions.remove(session);
    }

    public Connection getConnection() throws JMSException {
        checkClosed();
        return connection.getConnection();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " { " + connection + " }";
    }

    /**
     * Remove all of the temporary destinations created for this connection.
     * This is important since the underlying connection may be reused over a
     * long period of time, accumulating all of the temporary destinations from
     * each use. However, from the perspective of the lifecycle from the
     * client's view, close() closes the connection and, therefore, deletes all
     * of the temporary destinations created.
     */
    protected void cleanupConnectionTemporaryDestinations() {
        for (TemporaryQueue tempQueue : connTempQueues) {
            try {
                tempQueue.delete();
            } catch (JMSException ex) {
                LOG.info("failed to delete Temporary Queue \"" + tempQueue.toString() + "\" on closing pooled connection: " + ex.getMessage());
            }
        }
        connTempQueues.clear();

        for (TemporaryTopic tempTopic : connTempTopics) {
            try {
                tempTopic.delete();
            } catch (JMSException ex) {
                LOG.info("failed to delete Temporary Topic \"" + tempTopic.toString() + "\" on closing pooled connection: " + ex.getMessage());
            }
        }
        connTempTopics.clear();
    }

    /**
     * The PooledSession tracks all Sessions that it created and now we close them.  Closing the
     * PooledSession will return the internal Session to the Pool of Session after cleaning up
     * all the resources that the Session had allocated for this PooledConnection.
     */
    protected void cleanupAllLoanedSessions() {
        for (JmsPoolSession session : loanedSessions) {
            try {
                session.close();
            } catch (JMSException ex) {
                LOG.info("failed to close laoned Session \"" + session + "\" on closing pooled connection: " + ex.getMessage());
            }
        }

        loanedSessions.clear();
    }

    /**
     * @return the total number of Pooled session including idle sessions that are not
     *          currently loaned out to any client.
     *
     * @throws JMSException if the connection has been closed.
     */
    public int getNumSessions() throws JMSException {
        checkClosed();
        return this.connection.getNumSessions();
    }

    /**
     * @return the number of Sessions that are currently checked out of this Connection's session pool.
     *
     * @throws JMSException if the connection has been closed.
     */
    public int getNumActiveSessions() throws JMSException {
        checkClosed();
        return this.connection.getNumActiveSessions();
    }

    /**
     * @return the number of Sessions that are idle in this Connection's sessions pool.
     *
     * @throws JMSException if the connection has been closed.
     */
    public int getNumtIdleSessions() throws JMSException {
        checkClosed();
        return this.connection.getNumIdleSessions();
    }

    //----- Internal support methods -----------------------------------------//

    protected void checkClosed() throws IllegalStateException {
        if (closed.get()) {
            throw new IllegalStateException("Connection closed");
        }
    }
}
