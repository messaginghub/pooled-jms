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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

import org.junit.Test;
import org.messaginghub.pooled.jms.JmsPoolConnection;
import org.messaginghub.pooled.jms.JmsPoolSession;
import org.messaginghub.pooled.jms.JmsPoolSessionEventListener;
import org.messaginghub.pooled.jms.mock.MockJMSQueue;
import org.messaginghub.pooled.jms.mock.MockJMSSession;
import org.messaginghub.pooled.jms.mock.MockJMSSessionListener;
import org.messaginghub.pooled.jms.mock.MockJMSTemporaryQueue;
import org.messaginghub.pooled.jms.mock.MockJMSTemporaryTopic;
import org.messaginghub.pooled.jms.mock.MockJMSTopic;

public class JmsPoolSessionTest extends JmsPoolTestSupport {

    @Test(timeout = 60000)
    public void testToString() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session.toString());
        session.close();
        assertNotNull(session.toString());
    }

    @Test(timeout = 60000)
    public void testIsIgnoreClose() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertFalse(session.isIgnoreClose());
        session.setIgnoreClose(true);
        assertTrue(session.isIgnoreClose());
    }

    @Test(timeout = 60000)
    public void testIgnoreClose() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertFalse(session.isIgnoreClose());
        session.setIgnoreClose(true);
        assertTrue(session.isIgnoreClose());

        session.close();

        assertEquals(Session.AUTO_ACKNOWLEDGE, session.getAcknowledgeMode());
    }

    @Test(timeout = 60000)
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

    @Test(timeout = 60000)
    public void testGetXAResource() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        assertNull("Non-XA session should not return an XA Resource", session.getXAResource());

        session.close();

        try {
            session.getXAResource();
            fail("Session should be closed.");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 60000)
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

    @Test(timeout = 60000)
    public void testCloseOnTXSessionTriggersRollback() throws Exception {
        final AtomicBoolean rolledBack = new AtomicBoolean();

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(Session.SESSION_TRANSACTED);
        MockJMSSession mockSession = (MockJMSSession) session.getInternalSession();
        mockSession.addSessionListener(new MockJMSSessionListener() {

            @Override
            public void onSessionRollback(MockJMSSession session) throws JMSException {
                rolledBack.set(true);
            }
        });

        session.close();
        assertTrue("Session should rollback on close" , rolledBack.get());
    }

    @Test(timeout = 60000)
    public void testCloseWithErrorOnRollbackInvalidatesSession() throws Exception {
        final AtomicBoolean rolledBack = new AtomicBoolean();

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(Session.SESSION_TRANSACTED);

        assertEquals(1, connection.getNumSessions());
        assertEquals(0, connection.getNumtIdleSessions());

        MockJMSSession mockSession = (MockJMSSession) session.getInternalSession();
        mockSession.addSessionListener(new MockJMSSessionListener() {

            @Override
            public void onSessionRollback(MockJMSSession session) throws JMSException {
                rolledBack.set(true);

                throw new JMSException("Failed to rollback");
            }
        });

        session.close();

        assertTrue("Session should rollback on close" , rolledBack.get());

        assertEquals(0, connection.getNumSessions());
        assertEquals(0, connection.getNumtIdleSessions());
    }

    @Test(timeout = 60000)
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

    @Test(timeout = 60000)
    public void testCreateQueue() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getTestName());
        assertNotNull(queue);
        assertEquals(getTestName(), queue.getQueueName());
        assertTrue(queue instanceof MockJMSQueue);
    }

    @Test(timeout = 60000)
    public void testCreateTopic() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(getTestName());
        assertNotNull(topic);
        assertEquals(getTestName(), topic.getTopicName());
        assertTrue(topic instanceof MockJMSTopic);
    }

    @Test(timeout = 60000)
    public void testCreateTemporaryQueue() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue queue = session.createTemporaryQueue();
        assertNotNull(queue);
        assertTrue(queue instanceof MockJMSTemporaryQueue);
    }

    @Test(timeout = 60000)
    public void testCreateTemporaryTopic() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryTopic topic = session.createTemporaryTopic();
        assertNotNull(topic);
        assertTrue(topic instanceof MockJMSTemporaryTopic);
    }

    @Test(timeout = 60000)
    public void testGetXAResourceOnNonXAPooledSession() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNull(session.getXAResource());
    }

    @Test(timeout = 60000)
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

    @Test(timeout = 60000)
    public void testPooledSessionStatsOneSessionWithSessionLimit() throws Exception {
        cf.setMaximumActiveSessionPerConnection(1);
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

    @Test(timeout = 60000)
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

    @Test(timeout = 60000)
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

    @Test(timeout = 60000)
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

    @Test(timeout = 60000)
    public void testCreateDurableSubscriberWithSelectorAndNoLocal() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Topic topic = session.createTopic(getTestName());
        assertNotNull(session.createDurableSubscriber(topic, "name", "color = red", false));

        session.close();
        try {
            session.createDurableSubscriber(topic, "other-name", "color = greean", true);
            fail("Should not be able to createDurableSubscriber when closed");
        } catch (JMSException ex) {}
    }

    @Test(timeout = 60000)
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

    @Test(timeout = 60000)
    public void testCreateSubscriberWithSelectorAndNoLocal() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Topic topic = session.createTopic(getTestName());
        assertNotNull(session.createSubscriber(topic, "color = red", false));

        session.close();
        try {
            session.createSubscriber(topic, "color = greean", true);
            fail("Should not be able to createSubscriber when closed");
        } catch (JMSException ex) {}
    }

    @Test(timeout = 60000)
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

    @Test(timeout = 60000)
    public void testCreateReceiverWithSelector() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue = session.createQueue(getTestName());
        assertNotNull(session.createReceiver(queue, "color = red"));

        session.close();
        try {
            session.createReceiver(queue, "color = greean");
            fail("Should not be able to createReceiver when closed");
        } catch (JMSException ex) {}
    }

    @Test(timeout = 60000)
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
            session.setMessageListener((message) -> {});
            fail("Should not be able to setMessageListener when closed");
        } catch (JMSException ex) {}

        try {
            session.getMessageListener();
            fail("Should not be able to setMessageListener when closed");
        } catch (JMSException ex) {}
    }
}
