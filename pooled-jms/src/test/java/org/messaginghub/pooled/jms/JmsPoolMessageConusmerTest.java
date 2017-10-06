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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.Test;
import org.messaginghub.pooled.jms.JmsPoolConnection;

/**
 * Tests for the JMS Pool MessageConsumer wrapper
 */
public class JmsPoolMessageConusmerTest extends JmsPoolTestSupport {

    @Test
    public void testToString() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        Session session = connection.createSession();
        Queue queue = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(queue);

        assertNotNull(consumer.toString());
    }

    @Test
    public void testCloseMoreThanOnce() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        Session session = connection.createSession();
        Queue queue = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(queue);

        consumer.close();
        consumer.close();
    }

    @Test
    public void testReceive() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        Session session = connection.createSession();
        Queue queue = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(queue, "Color = Red");

        assertNull(consumer.receive());

        consumer.close();

        try {
            consumer.receive();
            fail("Should not be able to interact with closed consumer");
        } catch (IllegalStateException ise) {}
    }

    @Test
    public void testReceiveNoWait() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        Session session = connection.createSession();
        Queue queue = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(queue, "Color = Red");

        assertNull(consumer.receiveNoWait());

        consumer.close();

        try {
            consumer.receiveNoWait();
            fail("Should not be able to interact with closed consumer");
        } catch (IllegalStateException ise) {}
    }

    @Test
    public void testReceiveTimed() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        Session session = connection.createSession();
        Queue queue = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(queue, "Color = Red");

        assertNull(consumer.receive(1));

        consumer.close();

        try {
            consumer.receive(1);
            fail("Should not be able to interact with closed consumer");
        } catch (IllegalStateException ise) {}
    }

    @Test
    public void testGetMessageSelector() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        Session session = connection.createSession();
        Queue queue = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(queue, "Color = Red");

        assertNotNull(consumer.getMessageSelector());
        assertEquals("Color = Red", consumer.getMessageSelector());

        consumer.close();

        try {
            consumer.getMessageSelector();
            fail("Should not be able to interact with closed consumer");
        } catch (IllegalStateException ise) {}
    }

    @Test
    public void testSetMessageListener() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        Session session = connection.createSession();
        Queue queue = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(queue);

        MessageListener listener = new MessageListener() {

            @Override
            public void onMessage(Message message) {
            }
        };

        assertNull(consumer.getMessageListener());
        consumer.setMessageListener(listener);
        assertNotNull(consumer.getMessageListener());
        assertEquals(listener, consumer.getMessageListener());

        consumer.close();

        try {
            consumer.setMessageListener(null);
            fail("Should not be able to interact with closed consumer");
        } catch (IllegalStateException ise) {}

        try {
            consumer.getMessageListener();
            fail("Should not be able to interact with closed consumer");
        } catch (IllegalStateException ise) {}
    }
}
