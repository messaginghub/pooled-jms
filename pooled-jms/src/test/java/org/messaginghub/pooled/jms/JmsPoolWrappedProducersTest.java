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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;

import org.junit.Test;
import org.messaginghub.pooled.jms.mock.MockJMSConnection;
import org.messaginghub.pooled.jms.mock.MockJMSDefaultConnectionListener;
import org.messaginghub.pooled.jms.mock.MockJMSMessageProducer;
import org.messaginghub.pooled.jms.mock.MockJMSSession;

public class JmsPoolWrappedProducersTest extends JmsPoolTestSupport {

    @Test(timeout = 60000)
    public void testCreateMessageProducerWithAnonymousProducerEnabled() throws Exception {
        doTestCreateMessageProducer(true);
    }

    @Test(timeout = 60000)
    public void testCreateMessageProducerWithAnonymousProducerDisabled() throws Exception {
        doTestCreateMessageProducer(false);
    }

    private void doTestCreateMessageProducer(boolean useAnonymousProducers) throws JMSException {
        cf.setUseAnonymousProducers(useAnonymousProducers);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue1 = session.createTemporaryQueue();
        Queue queue2 = session.createTemporaryQueue();

        JmsPoolMessageProducer producer1 = (JmsPoolMessageProducer) session.createProducer(queue1);
        JmsPoolMessageProducer producer2 = (JmsPoolMessageProducer) session.createProducer(queue2);

        if (useAnonymousProducers) {
            assertSame(producer1.getMessageProducer(), producer2.getMessageProducer());
        } else {
            assertNotSame(producer1.getMessageProducer(), producer2.getMessageProducer());
        }

        connection.close();
    }

    @Test(timeout = 60000)
    public void testCreateAnonymousMessageProducerWithAnonymousProducerEnabled() throws Exception {
        doTestCreateAnonymousMessageProducer(true);
    }

    @Test(timeout = 60000)
    public void testCreateAnonymousMessageProducerWithAnonymousProducerDisabled() throws Exception {
        doTestCreateAnonymousMessageProducer(false);
    }

    private void doTestCreateAnonymousMessageProducer(boolean useAnonymousProducers) throws JMSException {
        cf.setUseAnonymousProducers(useAnonymousProducers);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        JmsPoolMessageProducer producer1 = (JmsPoolMessageProducer) session.createProducer(null);
        JmsPoolMessageProducer producer2 = (JmsPoolMessageProducer) session.createProducer(null);

        // Both cases should result in a single anonymous cached producer instance.
        assertSame(producer1.getMessageProducer(), producer2.getMessageProducer());

        connection.close();
    }

    @Test(timeout = 60000)
    public void testCreateTopicPublisherWithAnonymousProducerEnabled() throws Exception {
        doTestCreateTopicPublisher(true);
    }

    @Test(timeout = 60000)
    public void testCreateTopicPublisherWithAnonymousProducerDisabled() throws Exception {
        doTestCreateTopicPublisher(false);
    }

    private void doTestCreateTopicPublisher(boolean useAnonymousProducers) throws JMSException {
        cf.setUseAnonymousProducers(useAnonymousProducers);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

        Topic topic1 = session.createTopic("Topic-1");
        Topic topic2 = session.createTopic("Topic-2");

        JmsPoolTopicPublisher publisher1 = (JmsPoolTopicPublisher) session.createPublisher(topic1);
        JmsPoolTopicPublisher publisher2 = (JmsPoolTopicPublisher) session.createPublisher(topic2);

        if (useAnonymousProducers) {
            assertSame(publisher1.getMessageProducer(), publisher2.getMessageProducer());
        } else {
            assertNotSame(publisher1.getMessageProducer(), publisher2.getMessageProducer());
        }

        connection.close();
    }

    @Test(timeout = 60000)
    public void testCreateAnonymousTopicPublisherWithAnonymousProducerEnabled() throws Exception {
        doTestCreateAnonymousTopicPublisher(true);
    }

    @Test(timeout = 60000)
    public void testCreateAnonymousTopicPublisherWithAnonymousProducerDisabled() throws Exception {
        doTestCreateAnonymousTopicPublisher(false);
    }

    private void doTestCreateAnonymousTopicPublisher(boolean useAnonymousProducers) throws JMSException {
        cf.setUseAnonymousProducers(useAnonymousProducers);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

        JmsPoolTopicPublisher publisher1 = (JmsPoolTopicPublisher) session.createPublisher(null);
        JmsPoolTopicPublisher publisher2 = (JmsPoolTopicPublisher) session.createPublisher(null);

        // Both cases should result in a single anonymous cached producer instance.
        assertSame(publisher1.getMessageProducer(), publisher2.getMessageProducer());

        connection.close();
    }

    @Test(timeout = 60000)
    public void testCreateQueueSenderWithAnonymousProducerEnabled() throws Exception {
        doTestCreateQueueSender(true);
    }

    @Test(timeout = 60000)
    public void testCreateQueueSenderWithAnonymousProducerDisabled() throws Exception {
        doTestCreateQueueSender(false);
    }

    private void doTestCreateQueueSender(boolean useAnonymousProducers) throws JMSException {
        cf.setUseAnonymousProducers(useAnonymousProducers);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue1 = session.createTemporaryQueue();
        Queue queue2 = session.createTemporaryQueue();

        JmsPoolQueueSender sender1 = (JmsPoolQueueSender) session.createSender(queue1);
        JmsPoolQueueSender sender2 = (JmsPoolQueueSender) session.createSender(queue2);

        if (useAnonymousProducers) {
            assertSame(sender1.getMessageProducer(), sender2.getMessageProducer());
        } else {
            assertNotSame(sender1.getMessageProducer(), sender2.getMessageProducer());
        }

        connection.close();
    }

    @Test(timeout = 60000)
    public void testCreateAnonymousQueueSenderWithAnonymousProducerEnabled() throws Exception {
        doTestAnonymousCreateQueueSender(true);
    }

    @Test(timeout = 60000)
    public void testCreateAnonymousQueueSenderWithAnonymousProducerDisabled() throws Exception {
        doTestAnonymousCreateQueueSender(false);
    }

    private void doTestAnonymousCreateQueueSender(boolean useAnonymousProducers) throws JMSException {
        cf.setUseAnonymousProducers(useAnonymousProducers);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

        JmsPoolQueueSender sender1 = (JmsPoolQueueSender) session.createSender(null);
        JmsPoolQueueSender sender2 = (JmsPoolQueueSender) session.createSender(null);

        // Both cases should result in a single anonymous cached producer instance.
        assertSame(sender1.getMessageProducer(), sender2.getMessageProducer());

        connection.close();
    }

    @Test(timeout = 60000)
    public void testSendThrowsWhenProducerHasExplicitDestination() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue1 = session.createTemporaryQueue();
        Queue queue2 = session.createTemporaryQueue();

        JmsPoolMessageProducer producer = (JmsPoolMessageProducer) session.createProducer(queue1);

        try {
            producer.send(queue2, session.createTextMessage());
            fail("Should only be able to send to queue 1");
        } catch (Exception ex) {
        }

        try {
            producer.send(null, session.createTextMessage());
            fail("Should only be able to send to queue 1");
        } catch (Exception ex) {
        }
    }

    @Test(timeout = 60000)
    public void testCreateMessageProducerWithAnonymousProducerEnabledAndCacheSize() throws Exception {
        doTestCreateMessageProducerWithCacheSizeOption(true, 2);
    }

    @Test(timeout = 60000)
    public void testCreateMessageProducerWithAnonymousProducerDisabledAndCacheSize() throws Exception {
        doTestCreateMessageProducerWithCacheSizeOption(false, 2);
    }

    @Test(timeout = 60000)
    public void testCreateMessageProducerWithAnonymousProducerDisabledAndCacheSizeZero() throws Exception {
        doTestCreateMessageProducerWithCacheSizeOption(false, 0);
    }

    private void doTestCreateMessageProducerWithCacheSizeOption(boolean useAnonymousProducers, int explicitCacheSize) throws JMSException {
        cf.setUseAnonymousProducers(useAnonymousProducers);
        cf.setExplicitProducerCacheSize(explicitCacheSize);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue1 = session.createTemporaryQueue();
        Queue queue2 = session.createTemporaryQueue();

        JmsPoolMessageProducer producer1 = (JmsPoolMessageProducer) session.createProducer(queue1);
        JmsPoolMessageProducer producer2 = (JmsPoolMessageProducer) session.createProducer(queue2);

        if (useAnonymousProducers) {
            assertSame(producer1.getMessageProducer(), producer2.getMessageProducer());
            assertNull(producer1.getMessageProducer().getDestination());
            assertNull(producer2.getMessageProducer().getDestination());
        } else if (explicitCacheSize > 0) {
            assertNotSame(producer1.getMessageProducer(), producer2.getMessageProducer());
            assertNotNull(producer1.getMessageProducer().getDestination());
            assertNotNull(producer2.getMessageProducer().getDestination());

            JmsPoolMessageProducer producer3 = (JmsPoolMessageProducer) session.createProducer(queue1);
            JmsPoolMessageProducer producer4 = (JmsPoolMessageProducer) session.createProducer(queue2);

            assertSame(producer1.getMessageProducer(), producer3.getMessageProducer());
            assertSame(producer2.getMessageProducer(), producer4.getMessageProducer());
        } else {
            assertNotSame(producer1.getMessageProducer(), producer2.getMessageProducer());
            assertNotNull(producer1.getMessageProducer().getDestination());
            assertNotNull(producer2.getMessageProducer().getDestination());
        }

        connection.close();
    }

    @Test(timeout = 60000)
    public void testRemoteCloseOfPooledAnonymousMessageProducerCanRecover() throws Exception {
        final AtomicInteger produersCreated = new AtomicInteger();
        final AtomicInteger producersClosed = new AtomicInteger();

        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        MockJMSConnection mockConnection = (MockJMSConnection) connection.getConnection();
        mockConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

            @Override
            public void onCreateMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                produersCreated.incrementAndGet();
            }

            @Override
            public void onMessageSend(MockJMSSession session, MockJMSMessageProducer producer, Message message) throws JMSException {
                if (produersCreated.get() == 1) {
                    throw new IllegalStateException("Producer is closed");
                }
            }

            @Override
            public void onCloseMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                producersClosed.incrementAndGet();
            }
        });

        Session session = connection.createSession();
        Queue queue = session.createTemporaryQueue();
        MessageProducer producer1 = session.createProducer(queue);

        assertEquals(1, produersCreated.get());

        try {
            producer1.send(session.createTextMessage("test"));
            fail("Should have failed on send with IllegalStateException indicating the producer is closed.");
        } catch (IllegalStateException jmsISE) {
            assertEquals(1, producersClosed.get());
        }

        MessageProducer producer2 = session.createProducer(queue);

        assertEquals(2, produersCreated.get());

        producer1.close();
        producer2.close();

        assertEquals(1, producersClosed.get());
    }

    @Test(timeout = 60000)
    public void testRemoteCloseAfterSendOfPooledAnonymousMessageProducerCanRecover() throws Exception {
        final AtomicInteger produersCreated = new AtomicInteger();
        final AtomicInteger producersClosed = new AtomicInteger();

        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        MockJMSConnection mockConnection = (MockJMSConnection) connection.getConnection();
        mockConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

            @Override
            public void onCreateMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                produersCreated.incrementAndGet();
            }

            @Override
            public void onMessageSend(MockJMSSession session, MockJMSMessageProducer producer, Message message) throws JMSException {
                if (produersCreated.get() == 1) {
                    producer.close();
                }
            }

            @Override
            public void onCloseMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                producersClosed.incrementAndGet();
            }
        });

        Session session = connection.createSession();
        Queue queue = session.createTemporaryQueue();
        MessageProducer producer1 = session.createProducer(queue);

        // After send with delivery delay we reset the old value which can through if
        // the producer was to close in between.
        producer1.setDeliveryDelay(5000);

        assertEquals(1, produersCreated.get());

        try {
            producer1.send(session.createTextMessage("test"));
            fail("Should have failed on send with IllegalStateException indicating the producer is closed.");
        } catch (IllegalStateException jmsISE) {
            assertEquals(1, producersClosed.get());
        }

        MessageProducer producer2 = session.createProducer(queue);

        assertEquals(2, produersCreated.get());

        producer1.close();
        producer2.close();

        assertEquals(1, producersClosed.get());
    }

    @Test(timeout = 60000)
    public void testRemoteCloseAfterSendOfPooledAnonymousTopicPublisherCanRecover() throws Exception {
        final AtomicInteger produersCreated = new AtomicInteger();
        final AtomicInteger producersClosed = new AtomicInteger();

        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        MockJMSConnection mockConnection = (MockJMSConnection) connection.getConnection();
        mockConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

            @Override
            public void onCreateMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                produersCreated.incrementAndGet();
            }

            @Override
            public void onMessageSend(MockJMSSession session, MockJMSMessageProducer producer, Message message) throws JMSException {
                if (produersCreated.get() == 1) {
                    producer.close();
                }
            }

            @Override
            public void onCloseMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                producersClosed.incrementAndGet();
            }
        });

        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTemporaryTopic();
        TopicPublisher producer1 = session.createPublisher(topic);

        // After send with delivery delay we reset the old value which can through if
        // the producer was to close in between.
        producer1.setDeliveryDelay(5000);

        assertEquals(1, produersCreated.get());

        try {
            producer1.send(session.createTextMessage("test"));
            fail("Should have failed on send with IllegalStateException indicating the producer is closed.");
        } catch (IllegalStateException jmsISE) {
            assertEquals(1, producersClosed.get());
        }

        TopicPublisher producer2 = session.createPublisher(topic);

        assertEquals(2, produersCreated.get());

        producer1.close();
        producer2.close();

        assertEquals(1, producersClosed.get());
    }

    @Test(timeout = 60000)
    public void testRemoteCloseAfterSendOfPooledAnonymousQueueSenderCanRecover() throws Exception {
        final AtomicInteger produersCreated = new AtomicInteger();
        final AtomicInteger producersClosed = new AtomicInteger();

        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        MockJMSConnection mockConnection = (MockJMSConnection) connection.getConnection();
        mockConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

            @Override
            public void onCreateMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                produersCreated.incrementAndGet();
            }

            @Override
            public void onMessageSend(MockJMSSession session, MockJMSMessageProducer producer, Message message) throws JMSException {
                if (produersCreated.get() == 1) {
                    producer.close();
                }
            }

            @Override
            public void onCloseMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                producersClosed.incrementAndGet();
            }
        });

        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        QueueSender producer1 = session.createSender(queue);

        // After send with delivery delay we reset the old value which can through if
        // the producer was to close in between.
        producer1.setDeliveryDelay(5000);

        assertEquals(1, produersCreated.get());

        try {
            producer1.send(session.createTextMessage("test"));
            fail("Should have failed on send with IllegalStateException indicating the producer is closed.");
        } catch (IllegalStateException jmsISE) {
            assertEquals(1, producersClosed.get());
        }

        QueueSender producer2 = session.createSender(queue);

        assertEquals(2, produersCreated.get());

        producer1.close();
        producer2.close();

        assertEquals(1, producersClosed.get());
    }

    @Test(timeout = 60000)
    public void testCachedProducersAreClosedWhenSendTriggersIllegalStateException() throws Exception {
        cf.setUseAnonymousProducers(false);
        cf.setExplicitProducerCacheSize(10);

        final AtomicInteger produersCreated = new AtomicInteger();
        final AtomicInteger producersClosed = new AtomicInteger();

        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        MockJMSConnection mockConnection = (MockJMSConnection) connection.getConnection();
        mockConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

            @Override
            public void onCreateMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                produersCreated.incrementAndGet();
            }

            @Override
            public void onMessageSend(MockJMSSession session, MockJMSMessageProducer producer, Message message) throws JMSException {
                if (produersCreated.get() == 1) {
                    throw new IllegalStateException("Producer is closed");
                }
            }

            @Override
            public void onCloseMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                producersClosed.incrementAndGet();
            }
        });

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue1 = session.createTemporaryQueue();

        JmsPoolMessageProducer producer1 = (JmsPoolMessageProducer) session.createProducer(queue1);
        MessageProducer delegate1 = producer1.getDelegate();

        // Should return a wrapper whose underlying MessageProducer matches what we got from
        // the first create call as they underlying session will be the same.
        JmsPoolMessageProducer producer2 = (JmsPoolMessageProducer) session.createProducer(queue1);
        MessageProducer delegate2 = producer2.getDelegate();

        assertSame(delegate1, delegate2);

        assertEquals(1, produersCreated.get());

        try {
            producer1.send(session.createTextMessage("test"));
            fail("Should have failed on send with IllegalStateException indicating the producer is closed.");
        } catch (IllegalStateException jmsISE) {
            assertEquals(1, producersClosed.get());
        }

        JmsPoolMessageProducer producer3 = (JmsPoolMessageProducer) session.createProducer(queue1);
        MessageProducer delegate3 = producer3.getDelegate();

        assertEquals(2, produersCreated.get());

        assertNotSame(delegate1, delegate3);
        assertNotSame(delegate2, delegate3);

        producer1.close();
        producer2.close();
        producer3.close();

        assertEquals(1, producersClosed.get());
    }

    @Test(timeout = 60000)
    public void testCachedPublishersAreClosedWhenSendTriggersIllegalStateException() throws Exception {
        cf.setUseAnonymousProducers(false);
        cf.setExplicitProducerCacheSize(10);

        final AtomicInteger produersCreated = new AtomicInteger();
        final AtomicInteger producersClosed = new AtomicInteger();

        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        MockJMSConnection mockConnection = (MockJMSConnection) connection.getConnection();
        mockConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

            @Override
            public void onCreateMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                produersCreated.incrementAndGet();
            }

            @Override
            public void onMessageSend(MockJMSSession session, MockJMSMessageProducer producer, Message message) throws JMSException {
                if (produersCreated.get() == 1) {
                    throw new IllegalStateException("Producer is closed");
                }
            }

            @Override
            public void onCloseMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                producersClosed.incrementAndGet();
            }
        });

        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic1 = session.createTemporaryTopic();

        JmsPoolMessageProducer producer1 = (JmsPoolMessageProducer) session.createPublisher(topic1);
        MessageProducer delegate1 = producer1.getDelegate();

        // Should return a wrapper whose underlying MessageProducer matches what we got from
        // the first create call as they underlying session will be the same.
        JmsPoolMessageProducer producer2 = (JmsPoolMessageProducer) session.createPublisher(topic1);
        MessageProducer delegate2 = producer2.getDelegate();

        assertSame(delegate1, delegate2);

        assertEquals(1, produersCreated.get());

        try {
            producer1.send(session.createTextMessage("test"));
            fail("Should have failed on send with IllegalStateException indicating the producer is closed.");
        } catch (IllegalStateException jmsISE) {
            assertEquals(1, producersClosed.get());
        }

        JmsPoolMessageProducer producer3 = (JmsPoolMessageProducer) session.createPublisher(topic1);
        MessageProducer delegate3 = producer3.getDelegate();

        assertEquals(2, produersCreated.get());

        assertNotSame(delegate1, delegate3);
        assertNotSame(delegate2, delegate3);

        producer1.close();
        producer2.close();
        producer3.close();

        assertEquals(1, producersClosed.get());
    }

    @Test(timeout = 60000)
    public void testCachedSenderAreClosedWhenSendTriggersIllegalStateException() throws Exception {
        cf.setUseAnonymousProducers(false);
        cf.setExplicitProducerCacheSize(10);

        final AtomicInteger produersCreated = new AtomicInteger();
        final AtomicInteger producersClosed = new AtomicInteger();

        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        MockJMSConnection mockConnection = (MockJMSConnection) connection.getConnection();
        mockConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

            @Override
            public void onCreateMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                produersCreated.incrementAndGet();
            }

            @Override
            public void onMessageSend(MockJMSSession session, MockJMSMessageProducer producer, Message message) throws JMSException {
                if (produersCreated.get() == 1) {
                    throw new IllegalStateException("Producer is closed");
                }
            }

            @Override
            public void onCloseMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                producersClosed.incrementAndGet();
            }
        });

        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue1 = session.createTemporaryQueue();

        JmsPoolMessageProducer producer1 = (JmsPoolMessageProducer) session.createSender(queue1);
        MessageProducer delegate1 = producer1.getDelegate();

        // Should return a wrapper whose underlying MessageProducer matches what we got from
        // the first create call as they underlying session will be the same.
        JmsPoolMessageProducer producer2 = (JmsPoolMessageProducer) session.createSender(queue1);
        MessageProducer delegate2 = producer2.getDelegate();

        assertSame(delegate1, delegate2);

        assertEquals(1, produersCreated.get());

        try {
            producer1.send(session.createTextMessage("test"));
            fail("Should have failed on send with IllegalStateException indicating the producer is closed.");
        } catch (IllegalStateException jmsISE) {
            assertEquals(1, producersClosed.get());
        }

        JmsPoolMessageProducer producer3 = (JmsPoolMessageProducer) session.createSender(queue1);
        MessageProducer delegate3 = producer3.getDelegate();

        assertEquals(2, produersCreated.get());

        assertNotSame(delegate1, delegate3);
        assertNotSame(delegate2, delegate3);

        producer1.close();
        producer2.close();
        producer3.close();

        assertEquals(1, producersClosed.get());
    }
}
