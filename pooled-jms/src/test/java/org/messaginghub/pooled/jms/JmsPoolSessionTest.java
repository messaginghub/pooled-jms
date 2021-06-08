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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.IllegalStateException;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.messaginghub.pooled.jms.mock.MockJMSConnection;
import org.messaginghub.pooled.jms.mock.MockJMSDefaultConnectionListener;
import org.messaginghub.pooled.jms.mock.MockJMSDefaultSessionListener;
import org.messaginghub.pooled.jms.mock.MockJMSMessageProducer;
import org.messaginghub.pooled.jms.mock.MockJMSQueue;
import org.messaginghub.pooled.jms.mock.MockJMSSession;
import org.messaginghub.pooled.jms.mock.MockJMSTemporaryQueue;
import org.messaginghub.pooled.jms.mock.MockJMSTemporaryTopic;
import org.messaginghub.pooled.jms.mock.MockJMSTopic;

@Timeout(60)
public class JmsPoolSessionTest extends JmsPoolTestSupport {

    @Test
    public void testToString() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session.toString());
        session.close();
        assertNotNull(session.toString());
    }

    @Test
    public void testIsIgnoreClose() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertFalse(session.isIgnoreClose());
        session.setIgnoreClose(true);
        assertTrue(session.isIgnoreClose());
    }

    @Test
    public void testIgnoreClose() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertFalse(session.isIgnoreClose());
        session.setIgnoreClose(true);
        assertTrue(session.isIgnoreClose());

        session.close();

        assertEquals(Session.AUTO_ACKNOWLEDGE, session.getAcknowledgeMode());
    }

    @Test
    public void testRun() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        try {
            session.run();
            fail("Session should be unable to run outside EE.");
        } catch (JMSRuntimeException jmsre) {}

        session.close();

        try {
            session.run();
            fail("Session should be closed.");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test
    public void testGetXAResource() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        assertNull(session.getXAResource(), "Non-XA session should not return an XA Resource");

        session.close();

        try {
            session.getXAResource();
            fail("Session should be closed.");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test
    public void testClose() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        assertEquals(0, connection.getNumtIdleSessions());
        session.close();
        assertEquals(1, connection.getNumtIdleSessions());

        try {
            session.close();
        } catch (JMSException ex) {
            fail("Shouldn't fail on second close call.");
        }

        try {
            session.createTemporaryQueue();
            fail("Session should be closed.");
        } catch (IllegalStateException ise) {}
    }

    @Test
    public void testCloseOnTXSessionTriggersRollback() throws Exception {
        final AtomicBoolean rolledBack = new AtomicBoolean();

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(Session.SESSION_TRANSACTED);
        MockJMSSession mockSession = (MockJMSSession) session.getInternalSession();
        mockSession.addSessionListener(new MockJMSDefaultSessionListener() {

            @Override
            public void onSessionRollback(MockJMSSession session) throws JMSException {
                rolledBack.set(true);
            }
        });

        session.close();
        assertTrue(rolledBack.get(), "Session should rollback on close");
    }

    @Test
    public void testCloseWithErrorOnRollbackInvalidatesSession() throws Exception {
        final AtomicBoolean rolledBack = new AtomicBoolean();

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(Session.SESSION_TRANSACTED);

        assertEquals(1, connection.getNumSessions());
        assertEquals(0, connection.getNumtIdleSessions());

        MockJMSSession mockSession = (MockJMSSession) session.getInternalSession();
        mockSession.addSessionListener(new MockJMSDefaultSessionListener() {

            @Override
            public void onSessionRollback(MockJMSSession session) throws JMSException {
                rolledBack.set(true);

                throw new JMSException("Failed to rollback");
            }
        });

        session.close();

        assertTrue(rolledBack.get(), "Session should rollback on close");

        assertEquals(0, connection.getNumSessions());
        assertEquals(0, connection.getNumtIdleSessions());
    }

    @Test
    public void testConnectionCloseReflectedInSessionState() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        connection.close();

        try {
            session.getAcknowledgeMode();
            fail("Session should be closed.");
        } catch (IllegalStateException isre) {}

        try {
            session.run();
            fail("Session should be closed.");
        } catch (IllegalStateRuntimeException isre) {}

        try {
            session.createTemporaryQueue();
            fail("Session should be closed.");
        } catch (IllegalStateException ise) {}
    }

    @Test
    public void testCreateQueue() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getTestName());
        assertNotNull(queue);
        assertEquals(getTestName(), queue.getQueueName());
        assertTrue(queue instanceof MockJMSQueue);
    }

    @Test
    public void testCreateTopic() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(getTestName());
        assertNotNull(topic);
        assertEquals(getTestName(), topic.getTopicName());
        assertTrue(topic instanceof MockJMSTopic);
    }

    @Test
    public void testCreateTemporaryQueue() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue queue = session.createTemporaryQueue();
        assertNotNull(queue);
        assertTrue(queue instanceof MockJMSTemporaryQueue);
    }

    @Test
    public void testCreateTemporaryTopic() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryTopic topic = session.createTemporaryTopic();
        assertNotNull(topic);
        assertTrue(topic instanceof MockJMSTemporaryTopic);
    }

    @Test
    public void testGetXAResourceOnNonXAPooledSession() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNull(session.getXAResource());
    }

    @Test
    public void testPooledSessionStatsOneSession() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();

        assertEquals(0, connection.getNumActiveSessions());

        // Create one and check that stats follow
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertEquals(1, connection.getNumActiveSessions());
        session.close();

        // All back in the pool now
        assertEquals(0, connection.getNumActiveSessions());
        assertEquals(1, connection.getNumtIdleSessions());
        assertEquals(1, connection.getNumSessions());

        connection.close();
    }

    @Test
    public void testPooledSessionStatsOneSessionWithSessionLimit() throws Exception {
        cf.setMaxSessionsPerConnection(1);
        cf.setBlockIfSessionPoolIsFull(false);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();

        assertEquals(0, connection.getNumActiveSessions());

        // Create one and check that stats follow
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertEquals(1, connection.getNumActiveSessions());
        assertEquals(0, connection.getNumtIdleSessions());
        assertEquals(1, connection.getNumSessions());

        try {
            connection.createSession();
            fail("Should not be able to create new session");
        } catch (IllegalStateException ise) {}

        // Nothing should have changed as we didn't create anything.
        assertEquals(1, connection.getNumActiveSessions());
        assertEquals(0, connection.getNumtIdleSessions());
        assertEquals(1, connection.getNumSessions());

        session.close();

        // All back in the pool now
        assertEquals(0, connection.getNumActiveSessions());
        assertEquals(1, connection.getNumtIdleSessions());
        assertEquals(1, connection.getNumSessions());

        connection.close();
    }

    @Test
    public void testPooledSessionStatsTwoSessions() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();

        assertEquals(0, connection.getNumActiveSessions());

        // Create Two and check that stats follow
        Session session1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertEquals(2, connection.getNumActiveSessions());
        session1.close();
        assertEquals(1, connection.getNumActiveSessions());
        session2.close();

        // All back in the pool now.
        assertEquals(0, connection.getNumActiveSessions());
        assertEquals(2, connection.getNumtIdleSessions());
        assertEquals(2, connection.getNumSessions());

        connection.close();
    }

    @Test
    public void testAddSessionEventListener() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession();

        final CountDownLatch tempTopicCreated = new CountDownLatch(2);
        final CountDownLatch tempQueueCreated = new CountDownLatch(2);
        final CountDownLatch sessionClosed = new CountDownLatch(2);

        JmsPoolSessionEventListener listener = new JmsPoolSessionEventListener() {

            @Override
            public void onTemporaryTopicCreate(TemporaryTopic tempTopic) {
                tempTopicCreated.countDown();
            }

            @Override
            public void onTemporaryQueueCreate(TemporaryQueue tempQueue) {
                tempQueueCreated.countDown();
            }

            @Override
            public void onSessionClosed(JmsPoolSession session) {
                sessionClosed.countDown();
            }
        };

        session.addSessionEventListener(listener);
        session.addSessionEventListener(listener);

        assertNotNull(session.createTemporaryQueue());
        assertNotNull(session.createTemporaryTopic());

        session.close();

        assertEquals(1, tempQueueCreated.getCount());
        assertEquals(1, tempTopicCreated.getCount());
        assertEquals(1, sessionClosed.getCount());

        try {
            session.addSessionEventListener(listener);
            fail("Should throw on closed session.");
        } catch (IllegalStateException ise) {}
    }

    @Test
    public void testCreateDurableSubscriber() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Topic topic = session.createTopic(getTestName());
        assertNotNull(session.createDurableSubscriber(topic, "name"));

        session.close();
        try {
            session.createDurableSubscriber(topic, "name-2");
            fail("Should not be able to createDurableSubscriber when closed");
        } catch (JMSException ex) {}
    }

    @Test
    public void testCreateDurableSubscriberWithSelectorAndNoLocal() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Topic topic = session.createTopic(getTestName());
        assertNotNull(session.createDurableSubscriber(topic, "name", "color = red", false));

        session.close();
        try {
            session.createDurableSubscriber(topic, "other-name", "color = green", true);
            fail("Should not be able to createDurableSubscriber when closed");
        } catch (JMSException ex) {}
    }

    @Test
    public void testCreateSubscriber() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Topic topic = session.createTopic(getTestName());
        assertNotNull(session.createSubscriber(topic));

        session.close();
        try {
            session.createSubscriber(topic);
            fail("Should not be able to createSubscriber when closed");
        } catch (JMSException ex) {}
    }

    @Test
    public void testCreateSubscriberWithSelectorAndNoLocal() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Topic topic = session.createTopic(getTestName());
        assertNotNull(session.createSubscriber(topic, "color = red", false));

        session.close();
        try {
            session.createSubscriber(topic, "color = green", true);
            fail("Should not be able to createSubscriber when closed");
        } catch (JMSException ex) {}
    }

    @Test
    public void testCreateReceiver() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue = session.createQueue(getTestName());
        assertNotNull(session.createReceiver(queue));

        session.close();
        try {
            session.createReceiver(queue);
            fail("Should not be able to createReceiver when closed");
        } catch (JMSException ex) {}
    }

    @Test
    public void testCreateReceiverWithSelector() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue = session.createQueue(getTestName());
        assertNotNull(session.createReceiver(queue, "color = red"));

        session.close();
        try {
            session.createReceiver(queue, "color = green");
            fail("Should not be able to createReceiver when closed");
        } catch (JMSException ex) {}
    }

    @Test
    public void testSetMessageListener() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageListener listener = new MessageListener() {

            @Override
            public void onMessage(Message message) {
            }
        };

        session.setMessageListener(listener);
        assertSame(listener, session.getMessageListener());
        MockJMSSession mockSession = (MockJMSSession) session.getInternalSession();
        assertSame(listener, mockSession.getMessageListener());

        session.close();

        try {
            session.setMessageListener(new MessageListener() {

                @Override
                public void onMessage(Message message) {
                }
            });
            fail("Should not be able to setMessageListener when closed");
        } catch (JMSException ex) {}

        try {
            session.getMessageListener();
            fail("Should not be able to setMessageListener when closed");
        } catch (JMSException ex) {}
    }

    @Test
    public void testCachedProducersEnabledReturnsCached() throws Exception {
        cf.setUseAnonymousProducers(false);
        cf.setExplicitProducerCacheSize(2);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue1 = session.createTemporaryQueue();
        Queue queue2 = session.createTemporaryQueue();

        JmsPoolMessageProducer producer1 = (JmsPoolMessageProducer) session.createProducer(queue1);
        JmsPoolMessageProducer producer2 = (JmsPoolMessageProducer) session.createProducer(queue2);

        assertNotSame(producer1.getMessageProducer(), producer2.getMessageProducer());

        JmsPoolMessageProducer producer3 = (JmsPoolMessageProducer) session.createProducer(queue1);
        JmsPoolMessageProducer producer4 = (JmsPoolMessageProducer) session.createProducer(queue2);

        assertSame(producer1.getMessageProducer(), producer3.getMessageProducer());
        assertSame(producer2.getMessageProducer(), producer4.getMessageProducer());

        connection.close();
    }

    @Test
    public void testCachedProducersEnabledReturnsCachedOnNewSession() throws Exception {
        cf.setUseAnonymousProducers(false);
        cf.setExplicitProducerCacheSize(1);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue1 = session.createTemporaryQueue();

        JmsPoolMessageProducer producer1 = (JmsPoolMessageProducer) session.createProducer(queue1);

        session.close();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Should return a wrapper whose underlying MessageProducer matches what we got from
        // the first session as they underlying session will be the same.
        JmsPoolMessageProducer producer2 = (JmsPoolMessageProducer) session.createProducer(queue1);

        assertSame(producer1.getDelegate(), producer2.getDelegate());

        connection.close();
    }

    @Test
    public void testCachedProducersEnabledEvictsOldAndCreatesNew() throws Exception {
        cf.setUseAnonymousProducers(false);
        cf.setExplicitProducerCacheSize(2);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue1 = session.createTemporaryQueue();
        Queue queue2 = session.createTemporaryQueue();
        Queue queue3 = session.createTemporaryQueue();

        JmsPoolMessageProducer producer1 = (JmsPoolMessageProducer) session.createProducer(queue1);
        JmsPoolMessageProducer producer2 = (JmsPoolMessageProducer) session.createProducer(queue2);

        assertNotSame(producer1.getMessageProducer(), producer2.getMessageProducer());

        JmsPoolMessageProducer producer3 = (JmsPoolMessageProducer) session.createProducer(queue3);
        JmsPoolMessageProducer producer4 = (JmsPoolMessageProducer) session.createProducer(queue1);

        assertNotSame(producer1.getMessageProducer(), producer3.getMessageProducer());
        assertNotSame(producer2.getMessageProducer(), producer3.getMessageProducer());

        // Original MessageProducer should have been evicted from the cache and a new one created
        // for the second call to create a producer on Queue #1
        assertNotSame(producer1.getMessageProducer(), producer4.getMessageProducer());

        JmsPoolMessageProducer producer5 = (JmsPoolMessageProducer) session.createProducer(queue1);

        // Now we should be back to caching Queue #1
        assertNotSame(producer1.getMessageProducer(), producer5.getMessageProducer());

        connection.close();
    }

    @Test
    public void testCachedProducersEvictedFromCacheNotClosedUntilAllReferencesAreClosed() throws Exception {
        cf.setUseAnonymousProducers(false);
        cf.setExplicitProducerCacheSize(1);

        final AtomicBoolean producerClosed = new AtomicBoolean();

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue1 = session.createTemporaryQueue();
        Queue queue2 = session.createTemporaryQueue();

        final JmsPoolMessageProducer producer1 = (JmsPoolMessageProducer) session.createProducer(queue1);
        final JmsPoolMessageProducer producer2 = (JmsPoolMessageProducer) session.createProducer(queue1);

        MockJMSConnection mockConnection = (MockJMSConnection) connection.getConnection();
        mockConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

            @Override
            public void onCloseMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                if (producer.equals(producer1.getDelegate())) {
                    producerClosed.set(true);
                }
            }
        });

        assertSame(producer1.getMessageProducer(), producer2.getMessageProducer());

        // This replaces cached producer
        JmsPoolMessageProducer producer3 = (JmsPoolMessageProducer) session.createProducer(queue2);

        assertNotSame(producer1.getMessageProducer(), producer3.getMessageProducer());

        try {
            producer1.send(session.createMessage());
        } catch (JMSException ex) {
            fail("Should be able to send on evicted producer");
        }

        try {
            producer2.send(session.createMessage());
        } catch (JMSException ex) {
            fail("Should be able to send on evicted producer");
        }

        producer1.close();
        assertFalse(producerClosed.get());

        try {
            producer1.send(session.createMessage());
            fail("Should not be able to send on closed evicted producer");
        } catch (JMSException ex) {
        }

        try {
            producer2.send(session.createMessage());
        } catch (JMSException ex) {
            fail("Should be able to send on alternate evicted producer");
        }

        producer2.close();
        assertTrue(producerClosed.get());

        try {
            producer2.send(session.createMessage());
            fail("Should not be able to send on closed evicted producer");
        } catch (JMSException ex) {
        }

        connection.close();
    }

    @Test
    public void testCreateAnonymousProducerDoesNotEvictCachedExplicitProducer() throws Exception {
        cf.setUseAnonymousProducers(false);
        cf.setExplicitProducerCacheSize(1);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue = session.createTemporaryQueue();

        JmsPoolMessageProducer producer1 = (JmsPoolMessageProducer) session.createProducer(queue);
        JmsPoolMessageProducer producer2 = (JmsPoolMessageProducer) session.createProducer(queue);

        assertSame(producer1.getMessageProducer(), producer2.getMessageProducer());

        // Creating an anonymous producer should not use the same producer and on next create of
        // explicit producer we should get the cached version.
        JmsPoolMessageProducer producer3 = (JmsPoolMessageProducer) session.createProducer(null);

        assertNotSame(producer1.getMessageProducer(), producer3.getMessageProducer());
        assertNotSame(producer2.getMessageProducer(), producer3.getMessageProducer());

        JmsPoolMessageProducer producer4 = (JmsPoolMessageProducer) session.createProducer(queue);

        assertSame(producer1.getMessageProducer(), producer4.getMessageProducer());
        assertSame(producer2.getMessageProducer(), producer4.getMessageProducer());

        connection.close();
    }

    @Test
    public void testEvictedProducerClosedOnSessionClose() throws Exception {
        cf.setUseAnonymousProducers(false);
        cf.setExplicitProducerCacheSize(1);

        final AtomicBoolean producer1Closed = new AtomicBoolean(false);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue1 = session.createTemporaryQueue();
        Queue queue2 = session.createTemporaryQueue();

        // Producer 1 would enter the cache, and then be evicted by producer 2
        final JmsPoolMessageProducer producer1 = (JmsPoolMessageProducer) session.createProducer(queue1);
        final JmsPoolMessageProducer producer2 = (JmsPoolMessageProducer) session.createProducer(queue2);

        assertNotSame(producer1.getMessageProducer(), producer2.getMessageProducer());

        MockJMSConnection mockConnection = (MockJMSConnection) connection.getConnection();
        mockConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

            @Override
            public void onCloseMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                if (producer.equals(producer1.getDelegate())) {
                    producer1Closed.set(true);
                }
            }
        });

        session.close();

        assertTrue(producer1Closed.get());

        connection.close();
    }

    @Test
    public void testEvictionOfSeeminglyClosedSession() throws Exception {
        cf.setConnectionIdleTimeout(10);
        cf.setMaxSessionsPerConnection(2);
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();

        JmsPoolSession session1 = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MockJMSSession mockSession1 = (MockJMSSession) session1.getInternalSession();

        JmsPoolSession session2 = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MockJMSSession mockSession2 = (MockJMSSession) session2.getInternalSession();

        assertNotSame(mockSession1, mockSession2);

        session1.close();
        mockSession1.close();

        JmsPoolSession session3 = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MockJMSSession mockSession3 = (MockJMSSession) session3.getInternalSession();

        assertNotSame(mockSession1, mockSession3);
        assertNotSame(mockSession2, mockSession3);
    }
}
