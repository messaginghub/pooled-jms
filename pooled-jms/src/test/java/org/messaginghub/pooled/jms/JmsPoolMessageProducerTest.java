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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jms.CompletionListener;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.Test;
import org.messaginghub.pooled.jms.JmsPoolConnection;
import org.messaginghub.pooled.jms.mock.MockJMSDestination;
import org.messaginghub.pooled.jms.mock.MockJMSQueue;
import org.messaginghub.pooled.jms.mock.MockJMSTopic;

/**
 * Tests for the JMS Pool MessageProducer wrapper class.
 */
public class JmsPoolMessageProducerTest extends JmsPoolTestSupport {

    private final TestCompletionListener completionListener = new TestCompletionListener();

    @Test
    public void testToString() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        Session session = connection.createSession();
        Queue queue = session.createTemporaryQueue();
        MessageProducer producer = session.createProducer(queue);

        assertNotNull(producer.toString());
    }

    @Test
    public void testCloseMoreThanOnce() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        Session session = connection.createSession();
        Queue queue = session.createTemporaryQueue();
        MessageProducer producer = session.createProducer(queue);

        producer.close();
        producer.close();
    }

    @Test
    public void testSetDeliveryMode() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        Session session = connection.createSession();
        Queue queue = session.createTemporaryQueue();
        MessageProducer producer = session.createProducer(queue);

        assertEquals(Message.DEFAULT_DELIVERY_MODE, producer.getDeliveryMode());
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        assertEquals(DeliveryMode.NON_PERSISTENT, producer.getDeliveryMode());

        producer.close();

        try {
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            fail("Should throw when producer is closed.");
        } catch (IllegalStateException ise) {}

        try {
            producer.getDeliveryMode();
            fail("Should throw when producer is closed.");
        } catch (IllegalStateException ise) {}
    }

    @Test
    public void testSetDeliveryDelay() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();

        Session session = connection.createSession();
        Queue queue = session.createTemporaryQueue();
        MessageProducer producer = session.createProducer(queue);

        assertEquals(0, producer.getDeliveryDelay());
        producer.setDeliveryDelay(1);
        assertEquals(1, producer.getDeliveryDelay());

        producer.close();

        try {
            producer.setDeliveryDelay(0);
            fail("Should throw when producer is closed.");
        } catch (IllegalStateException ise) {}

        try {
            producer.getDeliveryDelay();
            fail("Should throw when producer is closed.");
        } catch (IllegalStateException ise) {}
    }

    @Test
    public void testSetPriority() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        Session session = connection.createSession();
        Queue queue = session.createTemporaryQueue();
        MessageProducer producer = session.createProducer(queue);

        assertEquals(Message.DEFAULT_PRIORITY, producer.getPriority());
        producer.setPriority(1);
        assertEquals(1, producer.getPriority());

        producer.close();

        try {
            producer.setPriority(0);
            fail("Should throw when producer is closed.");
        } catch (IllegalStateException ise) {}

        try {
            producer.getPriority();
            fail("Should throw when producer is closed.");
        } catch (IllegalStateException ise) {}
    }

    @Test
    public void testSetTimeToLive() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        Session session = connection.createSession();
        Queue queue = session.createTemporaryQueue();
        MessageProducer producer = session.createProducer(queue);

        assertEquals(Message.DEFAULT_TIME_TO_LIVE, producer.getTimeToLive());
        producer.setTimeToLive(1);
        assertEquals(1, producer.getTimeToLive());

        producer.close();

        try {
            producer.setTimeToLive(0);
            fail("Should throw when producer is closed.");
        } catch (IllegalStateException ise) {}

        try {
            producer.getTimeToLive();
            fail("Should throw when producer is closed.");
        } catch (IllegalStateException ise) {}
    }

    @Test
    public void testSetDisableMessageID() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        Session session = connection.createSession();
        Queue queue = session.createTemporaryQueue();
        MessageProducer producer = session.createProducer(queue);

        assertFalse(producer.getDisableMessageID());
        producer.setDisableMessageID(true);
        assertTrue(producer.getDisableMessageID());

        producer.close();

        try {
            producer.setDisableMessageID(false);
            fail("Should throw when producer is closed.");
        } catch (IllegalStateException ise) {}

        try {
            producer.getDisableMessageID();
            fail("Should throw when producer is closed.");
        } catch (IllegalStateException ise) {}
    }

    @Test
    public void testSetDisableTimestamp() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        Session session = connection.createSession();
        Queue queue = session.createTemporaryQueue();
        MessageProducer producer = session.createProducer(queue);

        assertFalse(producer.getDisableMessageTimestamp());
        producer.setDisableMessageTimestamp(true);
        assertTrue(producer.getDisableMessageTimestamp());

        producer.close();

        try {
            producer.setDisableMessageTimestamp(false);
            fail("Should throw when producer is closed.");
        } catch (IllegalStateException ise) {}

        try {
            producer.getDisableMessageTimestamp();
            fail("Should throw when producer is closed.");
        } catch (IllegalStateException ise) {}
    }

    @Test
    public void testNullDestinationOnSendToAnonymousProducer() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        Session session = connection.createSession();
        MessageProducer producer = session.createProducer(null);

        try {
            producer.send(null, session.createMessage());
            fail("Should not be able to send with null destination");
        } catch (InvalidDestinationException ide) {}
    }

    @Test
    public void testNullDestinationOnSendToTargetedProducer() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        Session session = connection.createSession();
        MessageProducer producer = session.createProducer(session.createTemporaryQueue());

        try {
            producer.send(null, session.createMessage());
            fail("Should not be able to send with null destination");
        } catch (InvalidDestinationException ide) {}
    }

    @Test
    public void testNullDestinationOnSendToAnonymousProducerWithCompletion() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        Session session = connection.createSession();
        MessageProducer producer = session.createProducer(null);
        CompletionListener listener = new CompletionListener() {

            @Override
            public void onException(javax.jms.Message message, Exception exception) {
            }

            @Override
            public void onCompletion(javax.jms.Message message) {
            }
        };

        try {
            producer.send(null, session.createMessage(), listener);
            fail("Should not be able to send with null destination");
        } catch (InvalidDestinationException ide) {}
    }

    @Test
    public void testNullDestinationOnSendToTargetedProducerWithCompletion() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        Session session = connection.createSession();
        MessageProducer producer = session.createProducer(session.createTemporaryQueue());
        CompletionListener listener = new CompletionListener() {

            @Override
            public void onException(javax.jms.Message message, Exception exception) {
            }

            @Override
            public void onCompletion(javax.jms.Message message) {
            }
        };

        try {
            producer.send(null, session.createMessage(), listener);
            fail("Should not be able to send with null destination");
        } catch (InvalidDestinationException ide) {}
    }

    @Test(timeout = 10000)
    public void testAnonymousProducerThrowsUOEWhenExplictDestinationNotProvided() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        Session session = connection.createSession();
        MessageProducer producer = session.createProducer(null);

        Message message = session.createMessage();
        try {
            producer.send(message);
            fail("Expected exception not thrown");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }

        try {
            producer.send(message, completionListener);
            fail("Expected exception not thrown");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }

        try {
            producer.send(message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            fail("Expected exception not thrown");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }

        try {
            producer.send(message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE, completionListener);
            fail("Expected exception not thrown");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }
    }

    @Test(timeout = 10000)
    public void testExplicitQueueProducerThrowsIDEWhenNullDestinationIsProvidedOnSend() throws Exception {
        doExplicitProducerThrowsIDEWhenNullDestinationIsProvidedOnSendTestImpl(new MockJMSQueue("explicitQueueDest"));
    }

    @Test(timeout = 10000)
    public void testExplicitTopicProducerThrowsIDEWhenInvalidDestinationIsProvidedOnSend() throws Exception {
        doExplicitProducerThrowsIDEWhenNullDestinationIsProvidedOnSendTestImpl(new MockJMSTopic("explicitTopicDest"));
    }

    private void doExplicitProducerThrowsIDEWhenNullDestinationIsProvidedOnSendTestImpl(MockJMSDestination explicitDest) throws JMSException {
        Destination invalildNullDest = null;
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        Session session = connection.createSession();
        MessageProducer producer = session.createProducer(explicitDest);

        Message message = session.createMessage();

        try {
            producer.send(invalildNullDest, message);
            fail("Expected exception to be thrown");
        } catch (InvalidDestinationException ide) {
            // expected
        }

        try {
            producer.send(invalildNullDest, message, completionListener);
            fail("Expected exception to be thrown");
        } catch (InvalidDestinationException ide) {
            // expected
        }

        try {
            producer.send(invalildNullDest, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            fail("Expected exception to be thrown");
        } catch (InvalidDestinationException ide) {
            // expected
        }

        try {
            producer.send(invalildNullDest, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE, completionListener);
            fail("Expected exception to be thrown");
        } catch (InvalidDestinationException ide) {
            // expected
        }
    }

    @Test(timeout = 10000)
    public void testExplicitProducerThrowsUOEWhenExplictDestinationIsProvided() throws Exception {
        Destination dest = new MockJMSQueue("explicitDestination");
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        Session session = connection.createSession();
        MessageProducer producer = session.createProducer(dest);

        Message message = session.createMessage();

        try {
            producer.send(dest, message);
            fail("Expected exception not thrown");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }

        try {
            producer.send(dest, message, completionListener);
            fail("Expected exception not thrown");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }

        try {
            producer.send(dest, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            fail("Expected exception not thrown");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }

        try {
            producer.send(dest, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE, completionListener);
            fail("Expected exception not thrown");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }
    }

    @Test(timeout = 10000)
    public void testAnonymousDestinationProducerThrowsIDEWhenNullDestinationIsProvided() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        Session session = connection.createSession();
        MessageProducer producer = session.createProducer(null);

        Message message = session.createMessage();

        try {
            producer.send(null, message);
            fail("Expected exception not thrown");
        } catch (InvalidDestinationException ide) {
            // expected
        }

        try {
            producer.send(null, message, completionListener);
            fail("Expected exception not thrown");
        } catch (InvalidDestinationException ide) {
            // expected
        }

        try {
            producer.send(null, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            fail("Expected exception not thrown");
        } catch (InvalidDestinationException ide) {
            // expected
        }

        try {
            producer.send(null, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE, completionListener);
            fail("Expected exception not thrown");
        } catch (InvalidDestinationException ide) {
            // expected
        }
    }

    @Test(timeout = 10000)
    public void testAnonymousProducerThrowsIAEWhenNullCompletionListenerProvided() throws Exception {
        Destination dest = new MockJMSQueue("explicitDestination");

        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        Session session = connection.createSession();
        MessageProducer producer = session.createProducer(null);

        Message message = session.createMessage();

        try {
            producer.send(dest, message, null);
            fail("Expected exception not thrown");
        } catch (IllegalArgumentException iae) {
            // expected
        }

        try {
            producer.send(dest, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE, null);
            fail("Expected exception not thrown");
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    @Test(timeout = 10000)
    public void testExplicitProducerThrowsIAEWhenNullCompletionListenerIsProvided() throws Exception {
        Destination dest = new MockJMSQueue("explicitDestination");
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        Session session = connection.createSession();
        MessageProducer producer = session.createProducer(dest);

        Message message = session.createMessage();

        try {
            producer.send(message, null);
            fail("Expected exception not thrown");
        } catch (IllegalArgumentException iae) {
            // expected
        }

        try {
            producer.send(message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE, null);
            fail("Expected exception not thrown");
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    //----- Test Support -----------------------------------------------------//

    private class TestCompletionListener implements CompletionListener {

        @Override
        public void onCompletion(Message message) {
        }

        @Override
        public void onException(Message message, Exception exception) {
        }
    }
}
