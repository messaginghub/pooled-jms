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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.junit.Test;
import org.messaginghub.pooled.jms.JmsPoolConnection;
import org.messaginghub.pooled.jms.JmsPoolQueueReceiver;
import org.messaginghub.pooled.jms.mock.MockJMSQueueReceiver;

/**
 * Tests for the JMS Pool QueueReceiver wrapper
 */
public class JmsPoolQueueReceiverTest extends JmsPoolTestSupport {

    @Test
    public void testToString() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        QueueReceiver receiver = session.createReceiver(queue);

        assertNotNull(receiver.toString());
    }

    @Test
    public void testGetQueue() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        QueueReceiver receiver = session.createReceiver(queue);

        assertNotNull(receiver.getQueue());
        assertSame(queue, receiver.getQueue());

        receiver.close();

        try {
            receiver.getQueue();
            fail("Cannot read topic on closed receiver");
        } catch (IllegalStateException ise) {}
    }

    @Test
    public void testGetTopicSubscriber() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createQueueConnection();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        JmsPoolQueueReceiver receiver = (JmsPoolQueueReceiver) session.createReceiver(queue);

        assertNotNull(receiver.getQueueReceiver());
        assertTrue(receiver.getQueueReceiver() instanceof MockJMSQueueReceiver);

        receiver.close();

        try {
            receiver.getQueueReceiver();
            fail("Cannot read state on closed receiver");
        } catch (IllegalStateException ise) {}
    }
}
