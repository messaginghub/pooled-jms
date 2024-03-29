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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.jms.DeliveryMode;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import jakarta.jms.TopicPublisher;
import jakarta.jms.TopicSession;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.messaginghub.pooled.jms.mock.MockJMSConnection;
import org.messaginghub.pooled.jms.mock.MockJMSDefaultConnectionListener;
import org.messaginghub.pooled.jms.mock.MockJMSMessageProducer;
import org.messaginghub.pooled.jms.mock.MockJMSSession;
import org.messaginghub.pooled.jms.mock.MockJMSTopicPublisher;

/**
 * Tests for the JMS Pool TopicPublisher wrapper.
 */
@Timeout(60)
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
        mockConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

            @Override
            public void onMessageSend(MockJMSSession session, MockJMSMessageProducer producer, Message message) throws JMSException {
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
        mockConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

            @Override
            public void onMessageSend(MockJMSSession session, MockJMSMessageProducer producer, Message message) throws JMSException {
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
        mockConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

            @Override
            public void onMessageSend(MockJMSSession session, MockJMSMessageProducer producer, Message message) throws JMSException {
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
        mockConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

            @Override
            public void onMessageSend(MockJMSSession session, MockJMSMessageProducer producer, Message message) throws JMSException {
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
