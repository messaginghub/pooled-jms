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
package org.messaginghub.pooled.jms.mock;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.InvalidClientIDException;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
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

import org.messaginghub.pooled.jms.util.JMSExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mock JMS Connection object used for test the JMS Pool
 */
public class MockJMSConnection implements Connection, TopicConnection, QueueConnection, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MockJMSConnection.class);

    private final MockJMSConnectionStats stats = new MockJMSConnectionStats();
    private final Map<String, MockJMSSession> sessions = new ConcurrentHashMap<>();
    private final Map<MockJMSTemporaryDestination, MockJMSTemporaryDestination> tempDestinations = new ConcurrentHashMap<>();

    private final Set<MockJMSConnectionListener> connectionListeners = new HashSet<>();

    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean connected = new AtomicBoolean();
    private final AtomicBoolean started = new AtomicBoolean();

    private final AtomicLong sessionIdGenerator = new AtomicLong();
    private final AtomicLong tempDestIdGenerator = new AtomicLong();

    private final AtomicReference<Exception> failureCause = new AtomicReference<>();

    private final ThreadPoolExecutor executor;
    private final UUID connectionId = UUID.randomUUID();
    private final MockJMSUser user;

    private ExceptionListener exceptionListener;
    private String clientID;
    private boolean explicitClientID;

    public MockJMSConnection(MockJMSUser user) {
        this.user = user;

        executor = new ThreadPoolExecutor(1, 1, 5, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
            new ThreadFactory() {

                @Override
                public Thread newThread(Runnable r) {
                    Thread newThread = new Thread(r, "MockJMSConnection Thread: " + connectionId);
                    newThread.setDaemon(true);
                    return newThread;
                }
            });

        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardOldestPolicy());
    }

    @Override
    public void start() throws JMSException {
        checkClosedOrFailed();
        ensureConnected();

        started.set(true);
    }

    @Override
    public void stop() throws JMSException {
        checkClosedOrFailed();
        ensureConnected();

        started.set(false);
    }

    @Override
    public void close() throws JMSException {
        if (closed.compareAndSet(false, true)) {
            connected.set(false);
            started.set(false);

            // Refuse any new work, and let any existing work complete.
            executor.shutdown();
        }
    }

    //----- Create Session ---------------------------------------------------//

    @Override
    public QueueSession createQueueSession(boolean transacted, int acknowledgeMode) throws JMSException {
        checkClosedOrFailed();
        ensureConnected();
        int ackMode = getSessionAcknowledgeMode(transacted, acknowledgeMode);
        MockJMSQueueSession result = new MockJMSQueueSession(getNextSessionId(), ackMode, this);
        addSession(result);
        if (started.get()) {
            result.start();
        }
        return result;
    }

    @Override
    public TopicSession createTopicSession(boolean transacted, int acknowledgeMode) throws JMSException {
        checkClosedOrFailed();
        ensureConnected();
        int ackMode = getSessionAcknowledgeMode(transacted, acknowledgeMode);
        MockJMSTopicSession result = new MockJMSTopicSession(getNextSessionId(), ackMode, this);
        addSession(result);
        if (started.get()) {
            result.start();
        }
        return result;
    }

    @Override
    public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
        checkClosedOrFailed();
        ensureConnected();
        int ackMode = getSessionAcknowledgeMode(transacted, acknowledgeMode);
        MockJMSSession result = new MockJMSSession(getNextSessionId(), ackMode, this);
        signalCreateSession(result);
        addSession(result);
        if (started.get()) {
            result.start();
        }
        return result;
    }

    @Override
    public Session createSession(int acknowledgeMode) throws JMSException {
        return createSession(acknowledgeMode == Session.SESSION_TRANSACTED ? true : false, acknowledgeMode);
    }

    @Override
    public Session createSession() throws JMSException {
        return createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    //----- Connection Consumer ----------------------------------------------//

    @Override
    public ConnectionConsumer createConnectionConsumer(Topic topic, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkClosedOrFailed();
        ensureConnected();
        throw new JMSException("Not Supported");
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkClosedOrFailed();
        ensureConnected();
        throw new JMSException("Not Supported");
    }

    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkClosedOrFailed();
        ensureConnected();
        throw new JMSException("Not Supported");
    }

    @Override
    public ConnectionConsumer createSharedDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkClosedOrFailed();
        ensureConnected();
        throw new JMSException("Not Supported");
    }

    @Override
    public ConnectionConsumer createSharedConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkClosedOrFailed();
        ensureConnected();
        throw new JMSException("Not Supported");
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Queue queue, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkClosedOrFailed();
        ensureConnected();
        throw new JMSException("Not Supported");
    }

    //----- Get and Set methods for Connection -------------------------------//

    @Override
    public String getClientID() throws JMSException {
        checkClosedOrFailed();
        return clientID;
    }

    @Override
    public void setClientID(String clientID) throws JMSException {
        checkClosedOrFailed();

        if (explicitClientID) {
            throw new IllegalStateException("The clientID has already been set");
        }
        if (clientID == null || clientID.isEmpty()) {
            throw new InvalidClientIDException("Cannot have a null or empty clientID");
        }
        if (connected.get()) {
            throw new IllegalStateException("Cannot set the client id once connected.");
        }

        setClientID(clientID, true);

        // We weren't connected if we got this far, we should now connect to ensure the
        // configured clientID is valid.
        initialize();
    }

    void setClientID(String clientID, boolean explicitClientID) {
        this.explicitClientID = explicitClientID;
        this.clientID = clientID;
    }

    @Override
    public ConnectionMetaData getMetaData() throws JMSException {
        return MockJMSConnectionMetaData.INSTANCE;
    }

    @Override
    public ExceptionListener getExceptionListener() throws JMSException {
        checkClosedOrFailed();
        return exceptionListener;
    }

    @Override
    public void setExceptionListener(ExceptionListener listener) throws JMSException {
        checkClosedOrFailed();
        this.exceptionListener = listener;
    }

    public String getUsername() throws JMSException {
        checkClosedOrFailed();
        return user.getUsername();
    }

    public String getPassword() throws JMSException {
        checkClosedOrFailed();
        return user.getPassword();
    }

    public UUID getConnectionId() throws JMSException {
        checkClosedOrFailed();
        return connectionId;
    }

    public boolean isStarted() throws JMSException {
        checkClosedOrFailed();
        return started.get();
    }

    public MockJMSUser getUser() throws JMSException {
        checkClosedOrFailed();
        return user;
    }

    public MockJMSConnectionStats getConnectionStats() {
        return stats;
    }

    public void addConnectionListener(MockJMSConnectionListener listener) throws JMSException {
        checkClosedOrFailed();
        if (listener != null) {
            connectionListeners.add(listener);
        }
    }

    public void removeConnetionListener(MockJMSConnectionListener listener) throws JMSException {
        checkClosedOrFailed();
        connectionListeners.remove(listener);
    }

    public boolean isClosed() {
        return closed.get();
    }

    //----- Mock Connection behavioral control -------------------------------//

    public void injectConnectionFailure(Exception error) throws JMSException {
        connectionFailed(error);

        injectConnectionError(error);

        if (!closed.get()) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    // TODO - Close down all connection resources.
                //                    try {
                //                        shutdown(ex);
                //                    } catch (JMSException e) {
                //                        LOG.warn("Exception during connection cleanup, " + e, e);
                //                    }

                    // Don't accept any more connection work but allow all pending work
                    // to complete in order to ensure notifications are sent to any blocked
                    // resources.
                    executor.shutdown();
                }
            });
        }
    }

    public void injectConnectionError(Exception error) throws JMSException {
        if (!closed.get() ) {
            if (this.exceptionListener != null) {
                final JMSException jmsError = JMSExceptionSupport.create(error);

                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        MockJMSConnection.this.exceptionListener.onException(jmsError);
                    }
                });
            } else {
                LOG.debug("Async exception with no exception listener: {}", error, error);
            }
        }
    }

    //----- Internal Utility Methods -----------------------------------------//

    protected String getNextSessionId() {
        return connectionId.toString() + sessionIdGenerator.incrementAndGet();
    }

    protected TemporaryQueue createTemporaryQueue() throws JMSException {
        String destinationName = connectionId.toString() + ":" + tempDestIdGenerator.incrementAndGet();
        MockJMSTemporaryQueue queue = new MockJMSTemporaryQueue(destinationName);
        signalCreateTemporaryDestination(queue);
        tempDestinations.put(queue, queue);
        queue.setConnection(this);
        stats.temporaryDestinationCreated(queue);
        return queue;
    }

    protected TemporaryTopic createTemporaryTopic() throws JMSException {
        String destinationName = connectionId.toString() + ":" + tempDestIdGenerator.incrementAndGet();
        MockJMSTemporaryTopic topic = new MockJMSTemporaryTopic(destinationName);
        signalCreateTemporaryDestination(topic);
        tempDestinations.put(topic, topic);
        topic.setConnection(this);
        stats.temporaryDestinationCreated(topic);
        return topic;
    }

    protected int getSessionAcknowledgeMode(boolean transacted, int acknowledgeMode) throws JMSException {
        int result = acknowledgeMode;
        if (!transacted && acknowledgeMode == Session.SESSION_TRANSACTED) {
            throw new JMSException("acknowledgeMode SESSION_TRANSACTED cannot be used for an non-transacted Session");
        }

        if (transacted) {
            result = Session.SESSION_TRANSACTED;
        } else if (acknowledgeMode < Session.SESSION_TRANSACTED || acknowledgeMode > Session.DUPS_OK_ACKNOWLEDGE){
            throw new JMSException("acknowledgeMode " + acknowledgeMode + " cannot be used for an non-transacted Session");
        }

        return result;
    }

    private boolean isConnected() throws JMSException {
        return connected.get();
    }

    protected void checkClosedOrFailed() throws JMSException {
        checkClosed();
        if (failureCause.get() != null) {
            throw JMSExceptionSupport.create("Connection has failed", failureCause.get());
        }
    }

    protected void checkClosed() throws IllegalStateException {
        if (closed.get()) {
            throw new IllegalStateException("The Connection is closed");
        }
    }

    protected void connectionFailed(Exception cause) {
        failureCause .compareAndSet(null, cause);
    }

    MockJMSConnection initialize() throws JMSException {
        if (explicitClientID) {
            ensureConnected();
        }

        return this;
    }

    private void ensureConnected() throws JMSException {
        if (isConnected() || closed.get()) {
            return;
        }

        synchronized(this.connectionId) {
            if (isConnected() || closed.get()) {
                return;
            }

            if (clientID == null || clientID.trim().isEmpty()) {
                throw new IllegalArgumentException("Client ID cannot be null or empty string");
            }

            if (!user.isValid()) {
                executor.shutdown();

                throw new JMSSecurityException(user.getFailureCause());
            }

            connected.set(true);
        }
    }

    void addSession(MockJMSSession session) {
        sessions.put(session.getSessionId(), session);
    }

    void removeSession(MockJMSSession session) throws JMSException {
        sessions.remove(session.getSessionId());
        signalCloseSession(session);
    }

    void deleteTemporaryDestination(MockJMSTemporaryDestination destination) throws JMSException {
        checkClosedOrFailed();

        try {
            for (MockJMSSession session : sessions.values()) {
                if (session.isDestinationInUse(destination)) {
                    throw new IllegalStateException("A consumer is consuming from the temporary destination");
                }
            }

            signalDeleteTemporaryDestination(destination);
            stats.temporaryDestinationDestroyed(destination);
            tempDestinations.remove(destination);
        } catch (Exception e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    boolean isTemporaryDestinationDeleted(MockJMSTemporaryDestination destination) {
        return !tempDestinations.containsKey(destination);
    }

    private void signalCreateSession(MockJMSSession session) throws JMSException {
        for (MockJMSConnectionListener listener : connectionListeners) {
            listener.onCreateSession(session);
        }
    }

    private void signalCloseSession(MockJMSSession session) throws JMSException {
        for (MockJMSConnectionListener listener : connectionListeners) {
            listener.onCloseSession(session);
        }
    }

    private void signalMessageSend(MockJMSSession session, Message message) throws JMSException {
        for (MockJMSConnectionListener listener : connectionListeners) {
            listener.onMessageSend(session, message);
        }
    }

    private void signalCreateTemporaryDestination(MockJMSTemporaryDestination destination) throws JMSException {
        for (MockJMSConnectionListener listener : connectionListeners) {
            if (destination.isQueue()) {
                listener.onCreateTemporaryQueue((MockJMSTemporaryQueue) destination);
            } else {
                listener.onCreateTemporaryTopic((MockJMSTemporaryTopic) destination);
            }
        }
    }

    private void signalDeleteTemporaryDestination(MockJMSTemporaryDestination destination) throws JMSException {
        for (MockJMSConnectionListener listener : connectionListeners) {
            if (destination.isQueue()) {
                listener.onDeleteTemporaryQueue((MockJMSTemporaryQueue) destination);
            } else {
                listener.onDeleteTemporaryTopic((MockJMSTemporaryTopic) destination);
            }
        }
    }

    private void signalCreateMessageConsumer(MockJMSSession session, MockJMSMessageConsumer consumer) throws JMSException {
        for (MockJMSConnectionListener listener : connectionListeners) {
            listener.onCreateMessageConsumer(session, consumer);
        }
    }

    private void signalCloseMessageConsumer(MockJMSSession session, MockJMSMessageConsumer consumer) throws JMSException {
        for (MockJMSConnectionListener listener : connectionListeners) {
            listener.onCloseMessageConsumer(session, consumer);
        }
    }

    //----- Event points for MockJMS resources -------------------------------//

    void onMessageSend(MockJMSSession session, Message message) throws JMSException {
        signalMessageSend(session, message);
    }

    void onMessageConsumerCreate(MockJMSSession session, MockJMSMessageConsumer consumer) throws JMSException {
        signalCreateMessageConsumer(session, consumer);
    }

    void onMessageConsumerClose(MockJMSSession session, MockJMSMessageConsumer consumer) throws JMSException {
        signalCloseMessageConsumer(session, consumer);
    }
}
