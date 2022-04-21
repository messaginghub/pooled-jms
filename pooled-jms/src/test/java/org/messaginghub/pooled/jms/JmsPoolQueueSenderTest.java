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
import jakarta.jms.Queue;
import jakarta.jms.QueueSender;
import jakarta.jms.QueueSession;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.messaginghub.pooled.jms.mock.MockJMSConnection;
import org.messaginghub.pooled.jms.mock.MockJMSDefaultConnectionListener;
import org.messaginghub.pooled.jms.mock.MockJMSMessageProducer;
import org.messaginghub.pooled.jms.mock.MockJMSQueueSender;
import org.messaginghub.pooled.jms.mock.MockJMSSession;

/**
 * Test for the JMS Pools QueueSender wrapper.
 */
@Timeout(60)
public class JmsPoolQueueSenderTest extends JmsPoolTestSupport {

    @Test
    public void testToString() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        QueueSender sender = session.createSender(queue);

        assertNotNull(sender.toString());
    }

    @Test
    public void testGetQueue() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        QueueSender sender = session.createSender(queue);

        assertNotNull(sender.getQueue());
        assertSame(queue, sender.getQueue());

        sender.close();

        try {
            sender.getQueue();
            fail("Cannot read topic on closed sender");
        } catch (IllegalStateException ise) {}
    }

    @Test
    public void testGetTopicSubscriber() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        JmsPoolQueueSender sender = (JmsPoolQueueSender) session.createSender(queue);

        assertNotNull(sender.getQueueSender());
        assertTrue(sender.getQueueSender() instanceof MockJMSQueueSender);

        sender.close();

        try {
            sender.getQueueSender();
            fail("Cannot read state on closed sender");
        } catch (IllegalStateException ise) {}
    }

    @Test
    public void testSendToQueue() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createTopicConnection();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        QueueSender sender = session.createSender(null);

        final AtomicBoolean sent = new AtomicBoolean();
        MockJMSConnection mockConnection = (MockJMSConnection) connection.getConnection();
        mockConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

            @Override
            public void onMessageSend(MockJMSSession session, MockJMSMessageProducer producer, Message message) throws JMSException {
                assertTrue(message instanceof TextMessage);
                sent.set(true);
            }
        });

        sender.send(queue, session.createTextMessage());

        assertTrue(sent.get());
    }

    @Test
    public void testSendToQueueWithOverrides() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createTopicConnection();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        QueueSender sender = session.createSender(null);

        final AtomicBoolean sent = new AtomicBoolean();
        MockJMSConnection mockConnection = (MockJMSConnection) connection.getConnection();
        mockConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

            @Override
            public void onMessageSend(MockJMSSession session, MockJMSMessageProducer producer, Message message) throws JMSException {
                assertEquals(DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());
                assertEquals(9, message.getJMSPriority());
                assertTrue(message.getJMSExpiration() != 0);

                sent.set(true);
            }
        });

        sender.send(queue, session.createTextMessage(), DeliveryMode.PERSISTENT, 9, 100);

        assertTrue(sent.get());
    }

    @Test
    public void testSendToQueueFailsIfNotAnonymousPublisher() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createTopicConnection();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        QueueSender sender = session.createSender(queue);

        try {
            sender.send(session.createTemporaryQueue(), session.createTextMessage());
            fail("Should not be able to send to alternate destination");
        } catch (UnsupportedOperationException ex) {}
    }
}
