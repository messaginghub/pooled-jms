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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.fail;

import jakarta.jms.Connection;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.messaginghub.pooled.jms.mock.MockJMSConnectionFactory;
import org.messaginghub.pooled.jms.mock.MockJMSUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for handling of cases of JMSSecurityException on create of Connection
 */
@Timeout(60)
public class JmsPoolConnectionSecurityExceptionTest extends JmsPoolTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(JmsPoolConnectionSecurityExceptionTest.class);

    private MockJMSUser user;

    @BeforeEach
    public void setUp() {
        user = new MockJMSUser("admin", "admin");

        factory = new MockJMSConnectionFactory();
        factory.addUser(user);

        cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(factory);
        cf.setMaxConnections(1);
        cf.start();
    }

    @Test
    public void testConnectionCreateAuthentication() throws JMSException {
        try {
            cf.createConnection("admin", "admin");
        } catch (JMSSecurityException jmsse) {
            fail("Should not be able to create connection using bad credentials");
        }

        assertEquals(1, cf.getNumConnections());
    }

    @Test
    public void testContextCreateAuthentication() throws JMSException {
        try {
            cf.createContext("admin", "admin");
        } catch (JMSRuntimeException jmsse) {
            fail("Should not be able to create connection using bad credentials");
        }

        assertEquals(1, cf.getNumConnections());
    }

    @Test
    public void testConnectionCreateAuthenticationError() throws JMSException {
        try {
            cf.createConnection("guest", "guest");
            fail("Should not be able to create connection using bad credentials");
        } catch (JMSSecurityException jmsse) {}

        assertEquals(0, cf.getNumConnections());
    }

    @Test
    public void testContextCreateAuthenticationError() throws JMSException {
        try {
            cf.createContext("guest", "guest");
            fail("Should not be able to create connection using bad credentials");
        } catch (JMSRuntimeException jmsse) {}

        assertEquals(0, cf.getNumConnections());
    }

    @Test
    public void testConnectionCreateWorksAfterAuthenticationError() throws JMSException {
        try {
            cf.createConnection("guest", "guest");
            fail("Should not be able to create connection using bad credentials");
        } catch (JMSSecurityException jmsse) {}

        assertEquals(0, cf.getNumConnections());

        try {
            Connection connection = cf.createConnection("admin", "admin");
            connection.close();
        } catch (JMSSecurityException jmsse) {
            fail("Should be able to create connection using bad credentials");
        }

        assertEquals(1, cf.getNumConnections());
    }

    @Test
    public void testContextCreateWorksAfterAuthenticationError() throws JMSException {
        try {
            cf.createContext("guest", "guest");
            fail("Should not be able to create connection using bad credentials");
        } catch (JMSRuntimeException jmsse) {}

        assertEquals(0, cf.getNumConnections());

        try {
            JMSContext context = cf.createContext("admin", "admin");
            context.close();
        } catch (JMSRuntimeException jmsse) {
            fail("Should be able to create connection using bad credentials");
        }

        assertEquals(1, cf.getNumConnections());
    }

    @Test
    public void testDeferredConnectionAuthenticationError() throws JMSException {
        // Don't throw on create fail on connection start
        factory.setDeferAuthenticationToConnection(true);

        Connection connection = null;
        try {
            connection = cf.createConnection("guest", "guest");
        } catch (JMSSecurityException jmsse) {
            fail("Should be able to create connection using bad credentials");
        }

        assertEquals(1, cf.getNumConnections());

        try {
            connection.start();
            fail("Should not be able to start connection using bad credentials");
        } catch (JMSSecurityException jmsse) {
        } finally {
            if (connection != null) {
                connection.close();
            }
        }

        // Try again, it should just hand back the original failed connection in this case.
        try {
            connection = cf.createConnection("guest", "guest");
        } catch (JMSSecurityException jmsse) {
            fail("Should be able to create connection using bad credentials");
        }

        assertEquals(1, cf.getNumConnections());

        // Try a new connection using valid credentials, the pool should create a new
        // Connection under the specified user / pass key which should work.
        try {
            cf.createConnection("admin", "admin");
        } catch (JMSSecurityException jmsse) {
            fail("Should not be able to create connection using bad credentials");
        }

        // We should have two now, the good one and the old failed one
        assertEquals(2, cf.getNumConnections());
    }

    @Test
    public void testDeferredConnectionAuthenticationErrorWithJMSContext() throws JMSException {
        // Don't throw on create fail on connection start
        factory.setDeferAuthenticationToConnection(true);

        JMSContext context = null;
        try {
            context = cf.createContext("guest", "guest");
        } catch (JMSRuntimeException jmsse) {
            fail("Should be able to create connection using bad credentials");
        }

        assertEquals(1, cf.getNumConnections());

        try {
            context.start();
            fail("Should not be able to start connection using bad credentials");
        } catch (JMSRuntimeException jmsse) {
        } finally {
            if (context != null) {
                context.close();
            }
        }

        // Try again, it should just hand back the original failed connection in this case.
        try {
            context = cf.createContext("guest", "guest");
        } catch (JMSRuntimeException jmsse) {
            fail("Should be able to create connection using bad credentials");
        }

        assertEquals(1, cf.getNumConnections());

        // Try a new connection using valid credentials, the pool should create a new
        // Connection under the specified user / pass key which should work.
        try {
            cf.createContext("admin", "admin");
        } catch (JMSRuntimeException jmsse) {
            fail("Should not be able to create connection using bad credentials");
        }

        // We should have two now, the good one and the old failed one
        assertEquals(2, cf.getNumConnections());
    }

    @Test
    public void testFailureGetsNewConnectionOnRetryBigPool() throws JMSException {
        // Don't throw on create fail on connection start
        factory.setDeferAuthenticationToConnection(true);
        cf.setMaxConnections(10);

        Connection connection1 = cf.createConnection("invalid", "credentials");
        try {
            connection1.start();
            fail("Should fail to connect");
        } catch (JMSSecurityException ex) {
            LOG.info("Caught expected security error");
        }

        Connection connection2 = cf.createConnection("invalid", "credentials");
        try {
            connection2.start();
            fail("Should fail to connect");
        } catch (JMSSecurityException ex) {
            LOG.info("Caught expected security error");
        }

        assertNotSame(connection1, connection2);

        connection1.close();
        connection2.close();
    }

    @Test
    public void testFailedCreateConsumerConnectionStillWorks() throws JMSException {
        // User can write but not read
        user.setCanConsumeAll(false);

        Connection connection = null;

        try {
            connection = cf.createConnection("admin", "admin");
        } catch (JMSSecurityException jmsse) {
            fail("Should not be able to create connection using bad credentials");
        }

        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("test");

        try {
            session.createConsumer(queue);
            fail("Should fail to create consumer");
        } catch (JMSSecurityException ex) {
            LOG.debug("Caught expected security error");
        }

        MessageProducer producer = session.createProducer(queue);
        producer.close();

        connection.close();
    }
}
