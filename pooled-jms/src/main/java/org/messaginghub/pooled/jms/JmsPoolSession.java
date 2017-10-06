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

import java.io.Serializable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.jms.XASession;
import javax.transaction.xa.XAResource;

import org.apache.commons.pool2.KeyedObjectPool;
import org.messaginghub.pooled.jms.pool.PooledSessionHolder;
import org.messaginghub.pooled.jms.pool.PooledSessionKey;
import org.messaginghub.pooled.jms.util.JMSExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsPoolSession implements Session, TopicSession, QueueSession, XASession, AutoCloseable {
    private static final transient Logger LOG = LoggerFactory.getLogger(JmsPoolSession.class);

    private final PooledSessionKey key;
    private final KeyedObjectPool<PooledSessionKey, PooledSessionHolder> sessionPool;
    private final CopyOnWriteArrayList<MessageProducer> producers = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<MessageConsumer> consumers = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<QueueBrowser> browsers = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<JmsPoolSessionEventListener> sessionEventListeners = new CopyOnWriteArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    private PooledSessionHolder sessionHolder;
    private boolean transactional = true;
    private boolean ignoreClose;
    private boolean isXa;
    private boolean useAnonymousProducers = true;

    public JmsPoolSession(PooledSessionKey key, PooledSessionHolder sessionHolder, KeyedObjectPool<PooledSessionKey, PooledSessionHolder> sessionPool, boolean transactional, boolean anonymous) {
        this.key = key;
        this.sessionHolder = sessionHolder;
        this.sessionPool = sessionPool;
        this.transactional = transactional;
        this.useAnonymousProducers = anonymous;
    }

    @Override
    public void close() throws JMSException {
        if (ignoreClose) {
            return;
        }

        if (closed.compareAndSet(false, true)) {
            boolean invalidate = false;
            try {
                // lets reset the session
                getInternalSession().setMessageListener(null);

                // Close any consumers, producers and browsers that may have been created.
                for (MessageConsumer consumer : consumers) {
                    consumer.close();
                }

                for (QueueBrowser browser : browsers) {
                    browser.close();
                }

                for (MessageProducer producer : producers) {
                    producer.close();
                }

                consumers.clear();
                producers.clear();
                browsers.clear();

                if (transactional && !isXa) {
                    try {
                        getInternalSession().rollback();
                    } catch (JMSException e) {
                        invalidate = true;
                        LOG.warn("Caught exception trying rollback() when putting session back into the pool, will invalidate. " + e, e);
                    }
                }
            } catch (JMSException ex) {
                invalidate = true;
                LOG.warn("Caught exception trying close() when putting session back into the pool, will invalidate. " + ex, ex);
            } finally {
                consumers.clear();
                browsers.clear();
                for (JmsPoolSessionEventListener listener : this.sessionEventListeners) {
                    listener.onSessionClosed(this);
                }
                sessionEventListeners.clear();
            }

            if (invalidate) {
                // lets close the session and not put the session back into the pool
                // instead invalidate it so the pool can create a new one on demand.
                if (sessionHolder != null) {
                    try {
                        sessionHolder.close();
                    } catch (JMSException e1) {
                        LOG.trace("Ignoring exception on close as discarding session: " + e1, e1);
                    }
                }

                try {
                    sessionPool.invalidateObject(key, sessionHolder);
                } catch (Exception e) {
                    LOG.trace("Ignoring exception on invalidateObject as discarding session: " + e, e);
                }
            } else {
                try {
                    sessionPool.returnObject(key, sessionHolder);
                } catch (Exception e) {
                    javax.jms.IllegalStateException illegalStateException = new javax.jms.IllegalStateException(e.toString());
                    illegalStateException.initCause(e);
                    throw illegalStateException;
                }
            }

            sessionHolder = null;
        }
    }

    //----- Destination factory methods --------------------------------------//

    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        TemporaryQueue result;

        result = getInternalSession().createTemporaryQueue();

        // Notify all of the listeners of the created temporary Queue.
        for (JmsPoolSessionEventListener listener : this.sessionEventListeners) {
            listener.onTemporaryQueueCreate(result);
        }

        return result;
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        TemporaryTopic result;

        result = getInternalSession().createTemporaryTopic();

        // Notify all of the listeners of the created temporary Topic.
        for (JmsPoolSessionEventListener listener : this.sessionEventListeners) {
            listener.onTemporaryTopicCreate(result);
        }

        return result;
    }

    @Override
    public Queue createQueue(String s) throws JMSException {
        return getInternalSession().createQueue(s);
    }

    @Override
    public Topic createTopic(String s) throws JMSException {
        return getInternalSession().createTopic(s);
    }

    //----- Message factory methods ------------------------------------------//

    @Override
    public BytesMessage createBytesMessage() throws JMSException {
        return getInternalSession().createBytesMessage();
    }

    @Override
    public MapMessage createMapMessage() throws JMSException {
        return getInternalSession().createMapMessage();
    }

    @Override
    public Message createMessage() throws JMSException {
        return getInternalSession().createMessage();
    }

    @Override
    public ObjectMessage createObjectMessage() throws JMSException {
        return getInternalSession().createObjectMessage();
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable serializable) throws JMSException {
        return getInternalSession().createObjectMessage(serializable);
    }

    @Override
    public StreamMessage createStreamMessage() throws JMSException {
        return getInternalSession().createStreamMessage();
    }

    @Override
    public TextMessage createTextMessage() throws JMSException {
        return getInternalSession().createTextMessage();
    }

    @Override
    public TextMessage createTextMessage(String s) throws JMSException {
        return getInternalSession().createTextMessage(s);
    }

    //----- Session management APIs ------------------------------------------//

    @Override
    public void unsubscribe(String s) throws JMSException {
        getInternalSession().unsubscribe(s);
    }

    @Override
    public int getAcknowledgeMode() throws JMSException {
        return getInternalSession().getAcknowledgeMode();
    }

    @Override
    public boolean getTransacted() throws JMSException {
        return getInternalSession().getTransacted();
    }

    @Override
    public void recover() throws JMSException {
        getInternalSession().recover();
    }

    @Override
    public void commit() throws JMSException {
        getInternalSession().commit();
    }

    @Override
    public void rollback() throws JMSException {
        getInternalSession().rollback();
    }

    @Override
    public XAResource getXAResource() {
        final PooledSessionHolder session;
        try {
            session = safeGetSessionHolder();
        } catch (JMSException e) {
            throw JMSExceptionSupport.createRuntimeException(e);
        }

        if (session.getSession() instanceof XASession) {
            return ((XASession) session.getSession()).getXAResource();
        }

        return null;
    }

    @Override
    public Session getSession() {
        return this;
    }

    //----- Java EE Session run entry point ----------------------------------//

    @Override
    public MessageListener getMessageListener() throws JMSException {
        return getInternalSession().getMessageListener();
    }

    @Override
    public void setMessageListener(MessageListener messageListener) throws JMSException {
        getInternalSession().setMessageListener(messageListener);
    }

    @Override
    public void run() {
        final PooledSessionHolder session;
        try {
            session = safeGetSessionHolder();
        } catch (JMSException e) {
            throw JMSExceptionSupport.createRuntimeException(e);
        }

        if (session != null) {
            session.getSession().run();
        }
    }

    //----- Consumer related methods -----------------------------------------//

    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        return addQueueBrowser(getInternalSession().createBrowser(queue));
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String selector) throws JMSException {
        return addQueueBrowser(getInternalSession().createBrowser(queue, selector));
    }

    @Override
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        return addConsumer(getInternalSession().createConsumer(destination));
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String selector) throws JMSException {
        return addConsumer(getInternalSession().createConsumer(destination, selector));
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String selector, boolean noLocal) throws JMSException {
        return addConsumer(getInternalSession().createConsumer(destination, selector, noLocal));
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String selector) throws JMSException {
        return addTopicSubscriber(getInternalSession().createDurableSubscriber(topic, selector));
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name, String selector, boolean noLocal) throws JMSException {
        return addTopicSubscriber(getInternalSession().createDurableSubscriber(topic, name, selector, noLocal));
    }

    @Override
    public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        return addTopicSubscriber(((TopicSession) getInternalSession()).createSubscriber(topic));
    }

    @Override
    public TopicSubscriber createSubscriber(Topic topic, String selector, boolean local) throws JMSException {
        return addTopicSubscriber(((TopicSession) getInternalSession()).createSubscriber(topic, selector, local));
    }

    @Override
    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        return addQueueReceiver(((QueueSession) getInternalSession()).createReceiver(queue));
    }

    @Override
    public QueueReceiver createReceiver(Queue queue, String selector) throws JMSException {
        return addQueueReceiver(((QueueSession) getInternalSession()).createReceiver(queue, selector));
    }

    //----- JMS 2.0 Subscriber creation API ----------------------------------//

    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) throws JMSException {
        PooledSessionHolder state = safeGetSessionHolder();
        state.getConnection().checkClientJMSVersionSupport(2, 0);
        return addConsumer(state.getSession().createSharedConsumer(topic, sharedSubscriptionName));
    }

    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName, String messageSelector) throws JMSException {
        PooledSessionHolder state = safeGetSessionHolder();
        state.getConnection().checkClientJMSVersionSupport(2, 0);
        return addConsumer(state.getSession().createSharedConsumer(topic, sharedSubscriptionName, messageSelector));
    }

    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name) throws JMSException {
        PooledSessionHolder state = safeGetSessionHolder();
        state.getConnection().checkClientJMSVersionSupport(2, 0);
        return addConsumer(state.getSession().createDurableConsumer(topic, name));
    }

    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
        PooledSessionHolder state = safeGetSessionHolder();
        state.getConnection().checkClientJMSVersionSupport(2, 0);
        return addConsumer(state.getSession().createDurableConsumer(topic, name, messageSelector, noLocal));
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name) throws JMSException {
        PooledSessionHolder state = safeGetSessionHolder();
        state.getConnection().checkClientJMSVersionSupport(2, 0);
        return addConsumer(state.getSession().createSharedDurableConsumer(topic, name));
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector) throws JMSException {
        PooledSessionHolder state = safeGetSessionHolder();
        state.getConnection().checkClientJMSVersionSupport(2, 0);
        return addConsumer(state.getSession().createSharedDurableConsumer(topic, name, messageSelector));
    }

    //----- Producer related methods -----------------------------------------//

    @Override
    public MessageProducer createProducer(Destination destination) throws JMSException {
        return addMessageProducer(getMessageProducer(destination), destination);
    }

    @Override
    public QueueSender createSender(Queue queue) throws JMSException {
        return addQueueSender(getQueueSender(queue), queue);
    }

    @Override
    public TopicPublisher createPublisher(Topic topic) throws JMSException {
        return addTopicPublisher(getTopicPublisher(topic), topic);
    }

    //----- Session configuration methods ------------------------------------//

    public void addSessionEventListener(JmsPoolSessionEventListener listener) throws JMSException {
        checkClosed();
        if (!sessionEventListeners.contains(listener)) {
            this.sessionEventListeners.add(listener);
        }
    }

    public Session getInternalSession() throws JMSException {
        return safeGetSessionHolder().getSession();
    }

    public void setIsXa(boolean isXa) {
        this.isXa = isXa;
    }

    public boolean isIgnoreClose() {
        return ignoreClose;
    }

    public void setIgnoreClose(boolean ignoreClose) {
        this.ignoreClose = ignoreClose;
    }

    @Override
    public String toString() {
        try {
            return getClass().getSimpleName() + " { " + safeGetSessionHolder() + " }";
        } catch (JMSException e) {
            return getClass().getSimpleName() + " { " + null + " }";
        }
    }

    //----- Consumer callback methods ----------------------------------------//

    /**
     * Callback invoked when the consumer is closed.
     * <p>
     * This is used to keep track of an explicit closed consumer created by this
     * session so that the internal tracking data structures can be cleaned up.
     *
     * @param consumer
     * 		the consumer which is being closed.
     */
    protected void onConsumerClose(MessageConsumer consumer) {
        consumers.remove(consumer);
    }

    /**
     * Callback invoked when the consumer is closed.
     * <p>
     * This is used to keep track of an explicit closed browser created by this
     * session so that the internal tracking data structures can be cleaned up.
     *
     * @param browser
     * 		the browser which is being closed.
     */
    protected void onQueueBrowserClose(QueueBrowser browser) {
        browsers.remove(browser);
    }

    /**
     * Callback invoked when the producer is closed.
     * <p>
     * This is used to keep track of an explicit closed producer created by this
     * session so that the internal tracking data structures can be cleaned up.
     *
     * @param producer
     * 		the producer which is being closed.
     */
    protected void onMessageProducerClosed(MessageProducer producer) {
        producers.remove(producer);
    }

    //----- Internal support methods -----------------------------------------//

    protected void checkClientJMSVersionSupport(int major, int minor) throws JMSException {
        safeGetSessionHolder().getConnection().checkClientJMSVersionSupport(major, minor);
    }

    protected boolean isJMSVersionSupported(int major, int minor) throws JMSException {
        return safeGetSessionHolder().getConnection().isJMSVersionSupported(major, minor);
    }

    private void checkClosed() throws IllegalStateException {
        if (closed.get()) {
            throw new IllegalStateException("Session is closed");
        }
    }

    private MessageProducer getMessageProducer(Destination destination) throws JMSException {
        MessageProducer result = null;

        if (useAnonymousProducers) {
            result = safeGetSessionHolder().getOrCreateProducer();
        } else {
            result = getInternalSession().createProducer(destination);
        }

        return result;
    }

    private QueueSender getQueueSender(Queue destination) throws JMSException {
        QueueSender result = null;

        if (useAnonymousProducers) {
            result = safeGetSessionHolder().getOrCreateSender();
        } else {
            result = ((QueueSession) getInternalSession()).createSender(destination);
        }

        return result;
    }

    private TopicPublisher getTopicPublisher(Topic destination) throws JMSException {
        TopicPublisher result = null;

        if (useAnonymousProducers) {
            result = safeGetSessionHolder().getOrCreatePublisher();
        } else {
            result = ((TopicSession) getInternalSession()).createPublisher(destination);
        }

        return result;
    }

    private QueueBrowser addQueueBrowser(QueueBrowser browser) {
        browser = new JmsPoolQueueBrowser(this, browser);
        browsers.add(browser);
        return browser;
    }

    private MessageConsumer addConsumer(MessageConsumer consumer) {
        consumer = new JmsPoolMessageConsumer(this, consumer);
        consumers.add(consumer);
        return consumer;
    }

    private TopicSubscriber addTopicSubscriber(TopicSubscriber subscriber) {
        subscriber = new JmsPoolTopicSubscriber(this, subscriber);
        consumers.add(subscriber);
        return subscriber;
    }

    private QueueReceiver addQueueReceiver(QueueReceiver receiver) {
        receiver = new JmsPoolQueueReceiver(this, receiver);
        consumers.add(receiver);
        return receiver;
    }

    private QueueSender addQueueSender(QueueSender sender, Queue queue) throws JMSException {
        sender = new JmsPoolQueueSender(this, sender, queue, useAnonymousProducers);
        producers.add(sender);
        return sender;
    }

    private TopicPublisher addTopicPublisher(TopicPublisher publisher, Topic topic) throws JMSException {
        publisher = new JmsPoolTopicPublisher(this, publisher, topic, useAnonymousProducers);
        producers.add(publisher);
        return publisher;
    }

    private MessageProducer addMessageProducer(MessageProducer producer, Destination destination) throws JMSException {
        producer = new JmsPoolMessageProducer(this, producer, destination, useAnonymousProducers);
        producers.add(producer);
        return producer;
    }

    private PooledSessionHolder safeGetSessionHolder() throws JMSException {
        PooledSessionHolder sessionHolder = this.sessionHolder;
        if (sessionHolder == null) {
            throw new IllegalStateException("The session has already been closed");
        }

        return sessionHolder;
    }
}
