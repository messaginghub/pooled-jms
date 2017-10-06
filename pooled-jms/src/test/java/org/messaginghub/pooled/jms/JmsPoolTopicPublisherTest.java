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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.DeliveryMode;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;

import org.junit.Test;
import org.messaginghub.pooled.jms.JmsPoolConnection;
import org.messaginghub.pooled.jms.JmsPoolTopicPublisher;
import org.messaginghub.pooled.jms.mock.MockJMSConnection;
import org.messaginghub.pooled.jms.mock.MockJMSConnectionListener;
import org.messaginghub.pooled.jms.mock.MockJMSSession;
import org.messaginghub.pooled.jms.mock.MockJMSTopicPublisher;

/**
 * Tests for the JMS Pool TopicPublisher wrapper.
 */
public class JmsPoolTopicPublisherTest extends JmsPoolTestSupport {

    @Test
    public void testToString() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createTopicConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTemporaryTopic();
        TopicPublisher publisher = session.createPublisher(topic);

        assertNotNull(publisher.toString());
    }

    @Test
    public void testGetTopic() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createTopicConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTemporaryTopic();
        TopicPublisher publisher = session.createPublisher(topic);

        assertNotNull(publisher.getTopic());
        assertSame(topic, publisher.getTopic());

        publisher.close();

        try {
            publisher.getTopic();
            fail("Cannot read topic on closed publisher");
        } catch (IllegalStateException ise) {}
    }

    @Test
    public void testGetTopicPublisher() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createTopicConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTemporaryTopic();
        JmsPoolTopicPublisher publisher = (JmsPoolTopicPublisher) session.createPublisher(topic);

        assertNotNull(publisher.getTopicPublisher());
        assertTrue(publisher.getTopicPublisher() instanceof MockJMSTopicPublisher);

        publisher.close();

        try {
            publisher.getTopicPublisher();
            fail("Cannot read state on closed publisher");
        } catch (IllegalStateException ise) {}
    }

    @Test
    public void testPublish() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createTopicConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTemporaryTopic();
        TopicPublisher publisher = session.createPublisher(topic);

        final AtomicBoolean published = new AtomicBoolean();
        MockJMSConnection mockConnection = (MockJMSConnection) connection.getConnection();
        mockConnection.addConnectionListener(new MockJMSConnectionListener() {

            @Override
            public void onMessageSend(MockJMSSession session, Message message) throws JMSException {
                assertTrue(message instanceof TextMessage);
                published.set(true);
            }
        });

        publisher.publish(session.createTextMessage());

        assertTrue(published.get());
    }

    @Test
    public void testPublishToTopic() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createTopicConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTemporaryTopic();
        TopicPublisher publisher = session.createPublisher(null);

        final AtomicBoolean published = new AtomicBoolean();
        MockJMSConnection mockConnection = (MockJMSConnection) connection.getConnection();
        mockConnection.addConnectionListener(new MockJMSConnectionListener() {

            @Override
            public void onMessageSend(MockJMSSession session, Message message) throws JMSException {
                assertTrue(message instanceof TextMessage);
                published.set(true);
            }
        });

        publisher.publish(topic, session.createTextMessage());

        assertTrue(published.get());
    }

    @Test
    public void testPublishToTopicFailsIfNotAnonymousPublisher() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createTopicConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTemporaryTopic();
        TopicPublisher publisher = session.createPublisher(topic);

        try {
            publisher.publish(session.createTemporaryTopic(), session.createTextMessage());
            fail("Should not be able to send to alternate destination");
        } catch (UnsupportedOperationException ex) {}
    }

    @Test
    public void testPublishWithOverrides() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createTopicConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTemporaryTopic();
        TopicPublisher publisher = session.createPublisher(topic);

        final AtomicBoolean published = new AtomicBoolean();
        MockJMSConnection mockConnection = (MockJMSConnection) connection.getConnection();
        mockConnection.addConnectionListener(new MockJMSConnectionListener() {

            @Override
            public void onMessageSend(MockJMSSession session, Message message) throws JMSException {
                assertEquals(DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());
                assertEquals(9, message.getJMSPriority());
                assertTrue(message.getJMSExpiration() != 0);

                published.set(true);
            }
        });

        publisher.publish(session.createTextMessage(), DeliveryMode.PERSISTENT, 9, 1000);

        assertTrue(published.get());
    }

    @Test
    public void testPublishTopicWithOverrides() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createTopicConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTemporaryTopic();
        TopicPublisher publisher = session.createPublisher(null);

        final AtomicBoolean published = new AtomicBoolean();
        MockJMSConnection mockConnection = (MockJMSConnection) connection.getConnection();
        mockConnection.addConnectionListener(new MockJMSConnectionListener() {

            @Override
            public void onMessageSend(MockJMSSession session, Message message) throws JMSException {
                assertEquals(DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());
                assertEquals(9, message.getJMSPriority());
                assertTrue(message.getJMSExpiration() != 0);

                published.set(true);
            }
        });

        publisher.publish(topic, session.createTextMessage(), DeliveryMode.PERSISTENT, 9, 1000);

        assertTrue(published.get());
    }
}
