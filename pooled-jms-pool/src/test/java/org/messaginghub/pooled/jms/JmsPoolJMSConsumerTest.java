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
import javax.jms.IllegalStateRuntimeException;
import javax.jms.JMSConsumer;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.MessageListener;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.messaginghub.pooled.jms.JmsPoolJMSContext;
import org.messaginghub.pooled.jms.mock.MockJMSConnection;
import org.messaginghub.pooled.jms.mock.MockJMSConnectionListener;
import org.messaginghub.pooled.jms.mock.MockJMSMessageConsumer;
import org.messaginghub.pooled.jms.mock.MockJMSSession;

/**
 *
 */
public class JmsPoolJMSConsumerTest extends JmsPoolTestSupport {

    private JmsPoolJMSContext context;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        context = (JmsPoolJMSContext) cf.createContext();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            context.close();
        } finally {
            super.tearDown();
        }
    }

    //----- Test basic functionality -----------------------------------------//

    @Test(timeout = 30000)
    public void testToString() throws JMSException {
        JMSConsumer consumer = context.createConsumer(context.createTemporaryQueue());
        assertNotNull(consumer.toString());
    }

    @Test
    public void testCloseMoreThanOnce() throws JMSException {
        JMSConsumer consumer = context.createConsumer(context.createTemporaryQueue());

        consumer.close();
        consumer.close();
    }

    @Test
    public void testReceive() throws JMSException {
        JMSConsumer consumer = context.createConsumer(context.createTemporaryQueue());

        assertNull(consumer.receive());

        consumer.close();

        try {
            consumer.receive();
            fail("Should not be able to interact with closed consumer");
        } catch (IllegalStateRuntimeException ise) {}
    }

    @Test
    public void testReceiveNoWait() throws JMSException {
        JMSConsumer consumer = context.createConsumer(context.createTemporaryQueue());

        assertNull(consumer.receiveNoWait());

        consumer.close();

        try {
            consumer.receiveNoWait();
            fail("Should not be able to interact with closed consumer");
        } catch (IllegalStateRuntimeException ise) {}
    }

    @Test
    public void testReceiveTimed() throws JMSException {
        JMSConsumer consumer = context.createConsumer(context.createTemporaryQueue());

        assertNull(consumer.receive(1));

        consumer.close();

        try {
            consumer.receive(1);
            fail("Should not be able to interact with closed consumer");
        } catch (IllegalStateRuntimeException ise) {}
    }

    @Test
    public void testGetMessageSelector() throws JMSException {
        JMSConsumer consumer = context.createConsumer(context.createTemporaryQueue(), "Color = Red");

        assertNotNull(consumer.getMessageSelector());
        assertEquals("Color = Red", consumer.getMessageSelector());

        consumer.close();

        try {
            consumer.getMessageSelector();
            fail("Should not be able to interact with closed consumer");
        } catch (IllegalStateRuntimeException ise) {}
    }

    @Test
    public void testSetMessageListener() throws JMSException {
        JMSConsumer consumer = context.createConsumer(context.createTemporaryQueue());

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
        } catch (IllegalStateRuntimeException ise) {}

        try {
            consumer.getMessageListener();
            fail("Should not be able to interact with closed consumer");
        } catch (IllegalStateRuntimeException ise) {}
    }

    @Test
    public void testReceiveBody() throws JMSException {
        JMSConsumer consumer = context.createConsumer(context.createTemporaryQueue());

        try {
            consumer.receiveBody(String.class);
            fail("Should not be able to interact with closed consumer");
        } catch (JMSRuntimeException ise) {}
    }

    @Test
    public void testReceiveBodyNoWait() throws JMSException {
        JMSConsumer consumer = context.createConsumer(context.createTemporaryQueue());

        try {
            consumer.receiveBodyNoWait(String.class);
            fail("Should not be able to interact with closed consumer");
        } catch (JMSRuntimeException ise) {}
    }

    @Test
    public void testReceiveBodyTimed() throws JMSException {
        JMSConsumer consumer = context.createConsumer(context.createTemporaryQueue());

        try {
            consumer.receiveBody(String.class, 1);
            fail("Should not be able to interact with closed consumer");
        } catch (JMSRuntimeException ise) {}
    }

    @Test
    public void testJMSExOnConsumerCloseConvertedToJMSREx() throws JMSException {
        JMSConsumer consumer = context.createConsumer(context.createTemporaryQueue());

        MockJMSConnection connection = (MockJMSConnection) context.getConnection();
        connection.addConnectionListener(new MockJMSConnectionListener() {

            @Override
            public void onCloseMessageConsumer(MockJMSSession session, MockJMSMessageConsumer consumer) throws JMSException {
                throw new IllegalStateException("Some failure");
            }
        });

        try {
            consumer.close();
            fail("Should throw on wrapped consumer throw");
        } catch (IllegalStateRuntimeException isre) {}
    }
}
