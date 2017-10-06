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

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import javax.jms.BytesMessage;
import javax.jms.CompletionListener;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
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

/**
 * Mock JMS Session implementation used for testing
 */
public class MockJMSSession implements Session, QueueSession, TopicSession, AutoCloseable {

    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean started = new AtomicBoolean();
    private final ReentrantLock sendLock = new ReentrantLock();

    private final Map<String, MockJMSMessageProducer> producers = new ConcurrentHashMap<>();
    private final Map<String, MockJMSMessageConsumer> consumers = new ConcurrentHashMap<>();

    private final Set<MockJMSSessionListener> sessionListeners = new HashSet<>();

    private final String sessionId;
    private final int sessionMode;
    private final MockJMSConnection connection;

    private final AtomicLong consumerIdGenerator = new AtomicLong();
    private final AtomicLong producerIdGenerator = new AtomicLong();

    private MessageListener messageListener;

    public MockJMSSession(String sessionId, int sessionMode, MockJMSConnection connection) {
        this.sessionMode = sessionMode;
        this.connection = connection;
        this.sessionId = sessionId;
    }

    @Override
    public void close() throws JMSException {
        if (closed.compareAndSet(false, true)) {
            connection.removeSession(this);

            for (MockJMSMessageProducer producer : producers.values()) {
                producer.close();
            }

            for (MockJMSMessageConsumer consumer : consumers.values()) {
                consumer.close();
            }

            for (MockJMSSessionListener listener : sessionListeners) {
                listener.onSessionClosed(this);
            }
        }
    }

    public void start() throws JMSException {
    }

    public void stop() throws JMSException {
    }

    @Override
    public boolean getTransacted() throws JMSException {
        checkClosed();
        return sessionMode == SESSION_TRANSACTED;
    }

    @Override
    public int getAcknowledgeMode() throws JMSException {
        checkClosed();
        return sessionMode;
    }

    @Override
    public void commit() throws JMSException {
        checkClosed();

        if (!getTransacted()) {
            throw new IllegalStateException("Session is not transacted");
        }

        for (MockJMSSessionListener listener : sessionListeners) {
            listener.onSessionCommit(this);
        }
    }

    @Override
    public void rollback() throws JMSException {
        checkClosed();

        if (!getTransacted()) {
            throw new IllegalStateException("Session is not transacted");
        }

        for (MockJMSSessionListener listener : sessionListeners) {
            listener.onSessionRollback(this);
        }
    }

    @Override
    public void recover() throws JMSException {
        checkClosed();
    }

    @Override
    public void unsubscribe(String name) throws JMSException {
        checkClosed();
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        checkClosed();
        return messageListener;
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {
        checkClosed();
        this.messageListener = listener;
    }

    public String getSessionId() {
        return sessionId;
    }

    public MockJMSConnection getConnection() {
        return connection;
    }

    public void addSessionListener(MockJMSSessionListener listener) throws JMSException {
        checkClosed();
        if (listener != null) {
            sessionListeners.add(listener);
        }
    }

    public void removeSessionListener(MockJMSSessionListener listener) throws JMSException {
        checkClosed();
        sessionListeners.remove(listener);
    }

    //----- Message Factory Methods ------------------------------------------//

    @Override
    public BytesMessage createBytesMessage() throws JMSException {
        checkClosed();
        return new MockJMSBytesMessage();
    }

    @Override
    public MapMessage createMapMessage() throws JMSException {
        checkClosed();
        return new MockJMSMapMessage();
    }

    @Override
    public Message createMessage() throws JMSException {
        checkClosed();
        return new MockJMSMessage();
    }

    @Override
    public ObjectMessage createObjectMessage() throws JMSException {
        checkClosed();
        return new MockJMSObjectMessage();
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable object) throws JMSException {
        checkClosed();
        MockJMSObjectMessage message = new MockJMSObjectMessage();
        message.setObject(object);
        return message;
    }

    @Override
    public StreamMessage createStreamMessage() throws JMSException {
        checkClosed();
        return new MockJMSStreamMessage();
    }

    @Override
    public TextMessage createTextMessage() throws JMSException {
        checkClosed();
        return new MockJMSTextMessage();
    }

    @Override
    public TextMessage createTextMessage(String text) throws JMSException {
        checkClosed();
        MockJMSTextMessage message = new MockJMSTextMessage();
        message.setText(text);
        return message;
    }

    //----- Producer Factory Methods -----------------------------------------//

    @Override
    public MessageProducer createProducer(Destination destination) throws JMSException {
        checkClosed();
        return new MockJMSMessageProducer(this, getNextProducerId(), (MockJMSDestination) destination);
    }

    @Override
    public QueueSender createSender(Queue queue) throws JMSException {
        checkClosed();
        return new MockJMSQueueSender(this, getNextProducerId(), (MockJMSDestination) queue);
    }

    @Override
    public TopicPublisher createPublisher(Topic topic) throws JMSException {
        checkClosed();
        return new MockJMSTopicPublisher(this, getNextProducerId(), (MockJMSDestination) topic);
    }

    //----- Consumer Factory Methods -----------------------------------------//

    @Override
    public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        return createSubscriber(topic, null, false);
    }

    @Override
    public TopicSubscriber createSubscriber(Topic topic, String messageSelector, boolean noLocal) throws JMSException {
        checkClosed();
        checkDestination(topic);
        return new MockJMSTopicSubscriber(this, getNextConsumerId(), (MockJMSDestination) topic, messageSelector, noLocal);
    }

    @Override
    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        return createReceiver(queue, null);
    }

    @Override
    public QueueReceiver createReceiver(Queue queue, String messageSelector) throws JMSException {
        checkClosed();
        checkDestination(queue);
        return new MockJMSQueueReceiver(this, getNextConsumerId(), (MockJMSDestination) queue, messageSelector);
    }

    @Override
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        checkClosed();
        return createConsumer(destination, null, false);
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
        checkClosed();
        return createConsumer(destination, messageSelector, false);
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) throws JMSException {
        checkClosed();
        checkDestination(destination);
        return new MockJMSMessageConsumer(this, getNextConsumerId(), (MockJMSDestination) destination, messageSelector, noLocal);
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException {
        return createDurableSubscriber(topic, name, null, false);
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
        checkClosed();
        checkDestination(topic);
        return new MockJMSTopicSubscriber(this, getNextConsumerId(), (MockJMSDestination) topic, messageSelector, noLocal);
    }

    //----- Browser Factory Methods ------------------------------------------//

    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        return createBrowser(queue, null);
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException {
        checkClosed();
        checkDestination(queue);
        return new MockJMSQueueBrowser(this, getNextConsumerId(), (MockJMSDestination) queue, messageSelector);
    }

    //----- Destination Factory Methods --------------------------------------//

    @Override
    public Queue createQueue(String queueName) throws JMSException {
        checkClosed();
        return new MockJMSQueue(queueName);
    }

    @Override
    public Topic createTopic(String topicName) throws JMSException {
        checkClosed();
        return new MockJMSTopic(topicName);
    }

    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        checkClosed();
        return connection.createTemporaryQueue();
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        checkClosed();
        return connection.createTemporaryTopic();
    }

    //----- JMS 2.0 Shared Consumer Creation ---------------------------------//

    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) throws JMSException {
        checkClosed();
        checkDestination(topic);
        return new MockJMSMessageConsumer(this, getNextConsumerId(), (MockJMSTopic) topic, null, false);
    }

    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName, String messageSelector) throws JMSException {
        checkClosed();
        checkDestination(topic);
        return new MockJMSMessageConsumer(this, getNextConsumerId(), (MockJMSTopic) topic, messageSelector, false);
    }

    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name) throws JMSException {
        checkClosed();
        checkDestination(topic);
        return new MockJMSMessageConsumer(this, getNextConsumerId(), (MockJMSTopic) topic, null, false);
    }

    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal)throws JMSException {
        checkClosed();
        checkDestination(topic);
        return new MockJMSMessageConsumer(this, getNextConsumerId(), (MockJMSTopic) topic, messageSelector, noLocal);
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name) throws JMSException {
        checkClosed();
        checkDestination(topic);
        return new MockJMSMessageConsumer(this, getNextConsumerId(), (MockJMSTopic) topic, null, false);
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector) throws JMSException {
        checkClosed();
        checkDestination(topic);
        return new MockJMSMessageConsumer(this, getNextConsumerId(), (MockJMSTopic) topic, messageSelector, false);
    }

    //----- JEE Session methods not implemented ------------------------------//

    @Override
    public void run() {
        throw new JMSRuntimeException("Not Supported");
    }

    //----- Session access points for producer and consumer ------------------//

    void send(MockJMSMessageProducer producer, Destination destination, Message message, int deliveryMode, int priority, long timeToLive, boolean disableMessageId, boolean disableTimestamp, long deliveryDelay, CompletionListener completionListener) throws JMSException {
        sendLock.lock();
        try {
            message.setJMSDeliveryMode(deliveryMode);
            message.setJMSPriority(priority);
            message.setJMSRedelivered(false);
            message.setJMSDestination(destination);

            long timeStamp = System.currentTimeMillis();
            boolean hasTTL = timeToLive > Message.DEFAULT_TIME_TO_LIVE;
            boolean hasDelay = deliveryDelay > Message.DEFAULT_DELIVERY_DELAY;

            if (!(message instanceof MockJMSMessage)) {
                throw new IllegalStateException("Mock JMS client cannot handle foreign messages");
            }

            if (!disableTimestamp) {
                message.setJMSTimestamp(timeStamp);
            } else {
                message.setJMSTimestamp(0);
            }

            if (hasTTL) {
                message.setJMSExpiration(timeStamp + timeToLive);
            } else {
                message.setJMSExpiration(0);
            }

            long messageSequence = producer.getNextMessageSequence();
            String messageId = null;
            if (!disableMessageId) {
                messageId = producer.getProducerId() + ":"+ messageSequence;
            }

            // Set the delivery time. Purposefully avoided doing this earlier so
            // that we use the 'outbound' JmsMessage object reference when
            // updating our own message instances, avoids using the interface
            // in case the JMS 1.1 Message API is actually being used due to
            // being on the classpath too.
            long deliveryTime = timeStamp;
            if (hasDelay) {
                deliveryTime = timeStamp + deliveryDelay;
            }

            message.setJMSDeliveryTime(deliveryTime);

            // Set the message ID
            message.setJMSMessageID(messageId);

            try {
                connection.onMessageSend(this, message);
            } catch (JMSException jmsEx) {
                // If the synchronous portion of the send fails the completion be
                // notified but might depending on the circumstances of the failures,
                // remove it from the queue and check if is is already completed
                // once we decide to add completion support to the mock
                throw jmsEx;
            }
        } finally {
            sendLock.unlock();
        }
    }

    void acknowledge() throws JMSException {
        // TODO Auto-generated method stub
    }

    static void validateSessionMode(int mode) {
        switch (mode) {
            case JMSContext.AUTO_ACKNOWLEDGE:
            case JMSContext.CLIENT_ACKNOWLEDGE:
            case JMSContext.DUPS_OK_ACKNOWLEDGE:
            case JMSContext.SESSION_TRANSACTED:
                return;
            default:
                throw new JMSRuntimeException("Invalid Session Mode: " + mode);
        }
    }

    //----- Internal Utility Methods -----------------------------------------//

    private static void checkDestination(Destination dest) throws InvalidDestinationException {
        if (dest == null) {
            throw new InvalidDestinationException("Destination cannot be null");
        }
    }

    private void checkClosed() throws JMSException {
        if (closed.get()) {
            throw new JMSException("Session is closed");
        }
    }

    boolean isDestinationInUse(MockJMSTemporaryDestination destination) {
        // TODO Auto-generated method stub
        return false;
    }

    protected String getNextConsumerId() {
        return getSessionId() + ":" + consumerIdGenerator.incrementAndGet();
    }

    protected String getNextProducerId() {
        return getSessionId() + ":" + producerIdGenerator.incrementAndGet();
    }

    protected void add(MockJMSMessageConsumer consumer) throws JMSException {
        connection.onMessageConsumerCreate(this, consumer);
        consumers.put(consumer.getConsumerId(), consumer);

        if (started.get()) {
            consumer.start();
        }
    }

    protected void remove(MockJMSMessageConsumer consumer) throws JMSException {
        consumers.remove(consumer.getConsumerId());
        connection.onMessageConsumerClose(this, consumer);

        for (MockJMSSessionListener listener : sessionListeners) {
            listener.onConsumerClose(this, consumer);
        }
    }

    protected void add(MockJMSMessageProducer producer) {
        producers.put(producer.getProducerId(), producer);
    }

    protected void remove(MockJMSMessageProducer producer) throws JMSException {
        producers.remove(producer.getProducerId());

        for (MockJMSSessionListener listener : sessionListeners) {
            listener.onProducerClose(this, producer);
        }
    }
}
