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

import javax.jms.IllegalStateRuntimeException;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.JMSSecurityRuntimeException;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.junit.Test;
import org.messaginghub.pooled.jms.JmsPoolJMSContext;
import org.messaginghub.pooled.jms.mock.MockJMSConnection;
import org.messaginghub.pooled.jms.mock.MockJMSContext;
import org.messaginghub.pooled.jms.mock.MockJMSUser;

/**
 * Tests for the JMS Pool JMSContext implementation.
 */
public class JmsPoolJMSContextTest extends JmsPoolTestSupport {

    @Test(timeout = 30000)
    public void testCreateContextCreatesConnection() {
        JMSContext context = cf.createContext();

        assertNotNull(context);
        assertTrue(context instanceof JmsPoolJMSContext);
        assertEquals(1, cf.getNumConnections());
    }

    @Test(timeout = 30000)
    public void testToString() {
        JMSContext context = cf.createContext();
        assertNotNull(context.toString());
    }

    @Test(timeout = 30000)
    public void testGetMetaData() {
        JMSContext context = cf.createContext();
        assertNotNull(context.getMetaData());

        context.close();

        try {
            context.getMetaData();
            fail("Should not be able to get MetaData from closed.");
        } catch (JMSRuntimeException jmsre) {}
    }

    @Test(timeout = 30000)
    public void testGetClientID() {
        JMSContext context = cf.createContext();
        assertNotNull(context.getClientID());

        context.close();

        try {
            context.getClientID();
            fail("Should not be able to get ClientID from closed.");
        } catch (JMSRuntimeException jmsre) {}
    }

    @Test(timeout = 30000)
    public void testGetExceptionListener() {
        JMSContext context = cf.createContext();
        assertNull(context.getExceptionListener());
        context.setExceptionListener((exception) -> {});
        assertNotNull(context.getExceptionListener());

        context.close();

        try {
            context.getExceptionListener();
            fail("Should not be able to get ExceptionListener from closed.");
        } catch (JMSRuntimeException jmsre) {}

        try {
            context.setExceptionListener((exception) -> {});
            fail("Should not be able to set ExceptionListener from closed.");
        } catch (JMSRuntimeException jmsre) {}
    }

    @Test(timeout = 30000)
    public void testGetConnectionAfterClosed() {
        JmsPoolJMSContext context = (JmsPoolJMSContext) cf.createContext();

        assertNotNull(context.getConnection());

        context.close();

        try {
            context.getConnection();
            fail("Should not be able to get connection from closed.");
        } catch (JMSRuntimeException jmsre) {}
    }

    @Test(timeout = 30000)
    public void testCreateQueue() {
        JMSContext context = cf.createContext();
        assertNotNull(context.createQueue(getTestName()));

        context.close();
        try {
            context.createQueue(getTestName());
            fail("Should not be able to create resource when context is closed");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 30000)
    public void testCreateTemporaryQueue() {
        JMSContext context = cf.createContext();
        assertNotNull(context.createTemporaryQueue());

        context.close();
        try {
            context.createTemporaryQueue();
            fail("Should not be able to create resource when context is closed");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 30000)
    public void testCreateTopic() {
        JMSContext context = cf.createContext();
        assertNotNull(context.createTopic(getTestName()));

        context.close();
        try {
            context.createTopic(getTestName());
            fail("Should not be able to create resource when context is closed");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 30000)
    public void testCreateTemporaryTopic() {
        JMSContext context = cf.createContext();
        assertNotNull(context.createTemporaryTopic());

        context.close();
        try {
            context.createTemporaryTopic();
            fail("Should not be able to create resource when context is closed");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 30000)
    public void testCreateMessage() {
        JMSContext context = cf.createContext();
        assertNotNull(context.createMessage());

        context.close();
        try {
            context.createMessage();
            fail("Should not be able to create resource when context is closed");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 30000)
    public void testCreateBytesMessage() {
        JMSContext context = cf.createContext();
        assertNotNull(context.createBytesMessage());

        context.close();
        try {
            context.createBytesMessage();
            fail("Should not be able to create resource when context is closed");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 30000)
    public void testCreateMapMessage() {
        JMSContext context = cf.createContext();
        assertNotNull(context.createMapMessage());

        context.close();
        try {
            context.createMapMessage();
            fail("Should not be able to create resource when context is closed");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 30000)
    public void testCreateObjectMessage() {
        JMSContext context = cf.createContext();
        assertNotNull(context.createObjectMessage());

        context.close();
        try {
            context.createObjectMessage();
            fail("Should not be able to create resource when context is closed");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 30000)
    public void testCreateObjectMessageWithPayload() {
        JMSContext context = cf.createContext();
        assertNotNull(context.createObjectMessage("body"));

        context.close();
        try {
            context.createObjectMessage("body");
            fail("Should not be able to create resource when context is closed");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 30000)
    public void testCreateStreamMessage() {
        JMSContext context = cf.createContext();
        assertNotNull(context.createStreamMessage());

        context.close();
        try {
            context.createStreamMessage();
            fail("Should not be able to create resource when context is closed");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 30000)
    public void testCreateTextMessage() {
        JMSContext context = cf.createContext();
        assertNotNull(context.createTextMessage());

        context.close();
        try {
            context.createTextMessage();
            fail("Should not be able to create resource when context is closed");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 30000)
    public void testCreateTextMessageWithPayload() {
        JMSContext context = cf.createContext();
        assertNotNull(context.createTextMessage("body"));

        context.close();
        try {
            context.createTextMessage("body");
            fail("Should not be able to create resource when context is closed");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 30000)
    public void testCreateProducer() {
        JMSContext context = cf.createContext();
        assertNotNull(context.createProducer());
        assertNotNull(context.createProducer());

        context.close();
        try {
            context.createProducer();
            fail("Should not be able to create resource when context is closed");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 30000)
    public void testCreateProducerAnonymousNotAuthorized() {
        MockJMSUser user = new MockJMSUser("user", "password");
        user.setCanProducerAnonymously(false);

        factory.addUser(user);

        JMSContext context = cf.createContext("user", "password");
        try {
            context.createProducer();
            fail("Should not be able to create producer when not authorized");
        } catch (JMSSecurityRuntimeException jmssre) {}
    }

    @Test(timeout = 30000)
    public void testCreateBrowser() {
        JMSContext context = cf.createContext();
        Queue queue = context.createQueue(getTestName());
        assertNotNull(context.createBrowser(queue));

        context.close();
        try {
            context.createBrowser(queue);
            fail("Should not be able to create resource when context is closed");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 30000)
    public void testCreateBrowserWithSelector() {
        JMSContext context = cf.createContext();
        Queue queue = context.createQueue(getTestName());
        assertNotNull(context.createBrowser(queue, "color = pink"));

        context.close();
        try {
            context.createBrowser(queue, "color = cyan");
            fail("Should not be able to create resource when context is closed");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 30000)
    public void testCreateConsumer() {
        JMSContext context = cf.createContext();
        Queue queue = context.createQueue(getTestName());
        assertNotNull(context.createConsumer(queue));

        context.close();
        try {
            context.createConsumer(queue);
            fail("Should not be able to create resource when context is closed");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 30000)
    public void testCreateConsumerWithSelector() {
        JMSContext context = cf.createContext();
        Queue queue = context.createQueue(getTestName());
        assertNotNull(context.createConsumer(queue, "color = red"));

        context.close();
        try {
            context.createConsumer(queue, "color = blue");
            fail("Should not be able to create resource when context is closed");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 30000)
    public void testCreateConsumerWithSelectorAndNoLocl() {
        JMSContext context = cf.createContext();
        Queue queue = context.createQueue(getTestName());
        assertNotNull(context.createConsumer(queue, "color = red", false));

        context.close();
        try {
            context.createConsumer(queue, "color = blue", true);
            fail("Should not be able to create resource when context is closed");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 30000)
    public void testSharedCreateConsumer() {
        JMSContext context = cf.createContext();
        Topic topic = context.createTopic(getTestName());
        assertNotNull(context.createSharedConsumer(topic, "name"));

        context.close();
        try {
            context.createSharedConsumer(topic, "name");
            fail("Should not be able to create resource when context is closed");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 30000)
    public void testSharedCreateConsumerWithSelector() {
        JMSContext context = cf.createContext();
        Topic topic = context.createTopic(getTestName());
        assertNotNull(context.createSharedConsumer(topic, "name", "color = yellow"));

        context.close();
        try {
            context.createSharedConsumer(topic, "name", "color = green");
            fail("Should not be able to create resource when context is closed");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 30000)
    public void testCreateDurableConsumer() {
        JMSContext context = cf.createContext();
        Topic topic = context.createTopic(getTestName());
        assertNotNull(context.createDurableConsumer(topic, "test"));

        context.close();
        try {
            context.createDurableConsumer(topic, "test");
            fail("Should not be able to create resource when context is closed");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 30000)
    public void testCreateSharedDurableConsumer() {
        JMSContext context = cf.createContext();
        Topic topic = context.createTopic(getTestName());
        assertNotNull(context.createSharedDurableConsumer(topic, "test"));

        context.close();
        try {
            context.createSharedDurableConsumer(topic, "test");
            fail("Should not be able to create resource when context is closed");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 30000)
    public void testCreateDurableConsumerWithSelector() {
        JMSContext context = cf.createContext();
        Topic topic = context.createTopic(getTestName());
        assertNotNull(context.createDurableConsumer(topic, "test", "color = red", true));

        context.close();
        try {
            context.createDurableConsumer(topic, "test", "color = red", true);
            fail("Should not be able to create resource when context is closed");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 30000)
    public void testCreateShareedDurableConsumerWithSelector() {
        JMSContext context = cf.createContext();
        Topic topic = context.createTopic(getTestName());
        assertNotNull(context.createSharedDurableConsumer(topic, "test", "color = red"));

        context.close();
        try {
            context.createSharedDurableConsumer(topic, "test", "color = red");
            fail("Should not be able to create resource when context is closed");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test(timeout = 30000)
    public void testCreateSubContextWithInvalidSessionMode() {
        JMSContext context = cf.createContext();

        try {
            context.createContext(9);
            fail("Should not be able to call with invliad mode.");
        } catch (JMSRuntimeException jmsre) {}
    }

    @Test(timeout = 30000)
    public void testCreateSubContextAfterParentClosed() {
        JMSContext context = cf.createContext();

        context.close();

        try {
            context.createContext(Session.AUTO_ACKNOWLEDGE);
            fail("Should not be able to call with invliad mode.");
        } catch (JMSRuntimeException jmsre) {}
    }

    @Test(timeout = 30000)
    public void testCreateContextOptOutConfiguration() {
        cf.setUseProviderJMSContext(true);

        JMSContext context = cf.createContext();

        assertNotNull(context);
        assertFalse(context instanceof JmsPoolJMSContext);
        assertTrue(context instanceof MockJMSContext);
        assertEquals(0, cf.getNumConnections());
    }

    @Test(timeout = 30000)
    public void testCreateContextOptOutConfigurationWithCredentials() {
        cf.setUseProviderJMSContext(true);

        JMSContext context = cf.createContext("user", "password");

        assertNotNull(context);
        assertFalse(context instanceof JmsPoolJMSContext);
        assertTrue(context instanceof MockJMSContext);
        assertEquals(0, cf.getNumConnections());
    }

    @Test(timeout = 30000)
    public void testCreateContextOptOutConfigurationUserName() {
        cf.setUseProviderJMSContext(true);

        JMSContext context = cf.createContext("user", null);

        assertNotNull(context);
        assertFalse(context instanceof JmsPoolJMSContext);
        assertTrue(context instanceof MockJMSContext);
        assertEquals(0, cf.getNumConnections());
    }

    @Test(timeout = 30000)
    public void testCreateContextFromExistingContext() {
        JmsPoolJMSContext context1 = (JmsPoolJMSContext) cf.createContext();
        JmsPoolJMSContext context2 = (JmsPoolJMSContext) context1.createContext(Session.AUTO_ACKNOWLEDGE);

        assertEquals(1, cf.getNumConnections());
        assertSame(context1.getConnection(), context2.getConnection());
    }

    @Test(timeout = 30000)
    public void testCloseContextAfterCreatingContextLeaveCreatedContextOpen() {
        JmsPoolJMSContext context1 = (JmsPoolJMSContext) cf.createContext();
        JmsPoolJMSContext context2 = (JmsPoolJMSContext) context1.createContext(Session.AUTO_ACKNOWLEDGE);

        assertEquals(1, cf.getNumConnections());
        assertSame(context1.getConnection(), context2.getConnection());

        context1.close();

        assertEquals(1, cf.getNumConnections());
        assertNotNull(context2.getClientID());
        assertNotNull(context2.createBrowser(context2.createQueue(getTestName())));
    }

    @Test(timeout = 30000)
    public void testGetTransacted() {
        JmsPoolJMSContext context1 = (JmsPoolJMSContext) cf.createContext();
        JmsPoolJMSContext context2 = (JmsPoolJMSContext) context1.createContext(Session.SESSION_TRANSACTED);

        assertFalse(context1.getTransacted());
        assertTrue(context2.getTransacted());
    }

    @Test(timeout = 30000)
    public void testCommit() {
        JmsPoolJMSContext context1 = (JmsPoolJMSContext) cf.createContext();
        JmsPoolJMSContext context2 = (JmsPoolJMSContext) context1.createContext(Session.SESSION_TRANSACTED);

        try {
            context1.commit();
            fail("Cannot commit a non-TX session");
        } catch (JMSRuntimeException jmsre) {}

        context2.commit();
    }

    @Test(timeout = 30000)
    public void testRollback() {
        JmsPoolJMSContext context1 = (JmsPoolJMSContext) cf.createContext();
        JmsPoolJMSContext context2 = (JmsPoolJMSContext) context1.createContext(Session.SESSION_TRANSACTED);

        try {
            context1.rollback();
            fail("Cannot rollback a non-TX session");
        } catch (JMSRuntimeException jmsre) {}

        context2.rollback();
    }

    @Test(timeout = 30000)
    public void testRecover() {
        JmsPoolJMSContext context = (JmsPoolJMSContext) cf.createContext();

        context.recover();
        context.close();

        try {
            context.recover();
            fail("Cannot recover from a closed resource");
        } catch (JMSRuntimeException jmsre) {}
    }

    @Test(timeout = 30000)
    public void testAcknowledgeAutoAckContext() {
        JmsPoolJMSContext context = (JmsPoolJMSContext) cf.createContext();

        context.acknowledge();
    }

    @Test(timeout = 30000)
    public void testUnsubscribe() {
        JmsPoolJMSContext context = (JmsPoolJMSContext) cf.createContext();

        context.unsubscribe("sub");
        context.close();

        try {
            context.unsubscribe("sub");
            fail("Cannot unsubscribe from a closed resource");
        } catch (JMSRuntimeException jmsre) {}
    }

    @Test(timeout = 30000)
    public void testAcknowledgeClientAckContext() {
        JmsPoolJMSContext context = (JmsPoolJMSContext) cf.createContext(Session.CLIENT_ACKNOWLEDGE);

        try {
            context.acknowledge();
            fail("Pooled Context cannot invoke an acknowledge at session level.");
        } catch (JMSRuntimeException jmsre) {}
    }

    @Test(timeout = 60000)
    public void testSetClientIDTwiceWithSameID() throws Exception {
        JMSContext context = cf.createContext();

        // test: call setClientID("newID") twice
        // this should be tolerated and not result in an exception
        context.setClientID("newID");

        try {
            context.setClientID("newID");
            context.start();
            context.close();
        } catch (IllegalStateRuntimeException ise) {
            LOG.error("Repeated calls to newID2.setClientID(\"newID\") caused " + ise.getMessage());
            fail("Repeated calls to newID2.setClientID(\"newID\") caused " + ise.getMessage());
        } finally {
            cf.stop();
        }

        LOG.debug("Test finished.");
    }

    @Test(timeout = 60000)
    public void testSetClientIDTwiceWithDifferentID() throws Exception {
        JMSContext context = cf.createContext();

        // test: call setClientID() twice with different IDs
        // this should result in an IllegalStateException
        context.setClientID("newID1");
        try {
            context.setClientID("newID2");
            fail("calling Connection.setClientID() twice with different clientID must raise an IllegalStateException");
        } catch (IllegalStateRuntimeException ise) {
            LOG.debug("Correctly received " + ise);
        } finally {
            context.close();
            cf.stop();
        }

        LOG.debug("Test finished.");
    }

    @Test(timeout = 60000)
    public void testSetClientIDAfterConnectionStart() throws Exception {
        JMSContext context = cf.createContext();

        // test: try to call setClientID() after start()
        // should result in an exception
        try {
            context.start();
            context.setClientID("newID3");
            fail("Calling setClientID() after start() mut raise a JMSException.");
        } catch (IllegalStateRuntimeException ise) {
            LOG.debug("Correctly received " + ise);
        } finally {
            context.close();
            cf.stop();
        }

        LOG.debug("Test finished.");
    }

    @Test(timeout = 30000)
    public void testAutStartByDefault() throws JMSException {
        JmsPoolJMSContext context = (JmsPoolJMSContext) cf.createContext();
        assertNotNull(context.createConsumer(context.createQueue(getTestName())));

        MockJMSConnection connection = (MockJMSConnection) context.getConnection();
        assertTrue(connection.isStarted());
    }

    @Test(timeout = 30000)
    public void testAutoStartCanBeDisabled() throws JMSException {
        JmsPoolJMSContext context = (JmsPoolJMSContext) cf.createContext();
        context.setAutoStart(false);

        MockJMSConnection connection = (MockJMSConnection) context.getConnection();
        assertFalse(connection.isStarted());

        assertNotNull(context.createConsumer(context.createQueue(getTestName())));
        assertFalse(connection.isStarted());
        assertNotNull(context.createBrowser(context.createQueue(getTestName())));
        assertFalse(connection.isStarted());
    }

    @Test(timeout = 30000)
    public void testStartStopConnection() throws JMSException {
        JmsPoolJMSContext context = (JmsPoolJMSContext) cf.createContext();
        context.setAutoStart(false);
        assertNotNull(context.createConsumer(context.createQueue(getTestName())));

        MockJMSConnection connection = (MockJMSConnection) context.getConnection();
        assertFalse(connection.isStarted());

        context.start();
        assertTrue(connection.isStarted());

        // We cannot stop a JMS Connection from the pool as it is a shared resource.
        context.stop();
        assertTrue(connection.isStarted());
        context.close();

        try {
            context.stop();
            fail("Cannot call stop on a closed context.");
        } catch (JMSRuntimeException jmsre) {}
    }
}
