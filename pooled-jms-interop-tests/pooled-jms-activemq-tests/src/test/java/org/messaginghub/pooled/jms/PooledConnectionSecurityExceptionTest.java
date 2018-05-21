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

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.messaginghub.pooled.jms.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Pooled connections ability to handle security exceptions
 */
public class PooledConnectionSecurityExceptionTest extends ActiveMQJmsPoolTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(PooledConnectionSecurityExceptionTest.class);

    protected JmsPoolConnectionFactory pooledConnFact;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        pooledConnFact = createPooledConnectionFactory();
        pooledConnFact.setMaxConnections(1);
        pooledConnFact.setReconnectOnException(true);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            pooledConnFact.stop();
            pooledConnFact = null;
        } catch (Exception ex) {}

        super.tearDown();
    }

    @Test
    public void testFailedConnectThenSucceeds() throws JMSException {
        Connection connection = pooledConnFact.createConnection("invalid", "credentials");

        try {
            connection.start();
            fail("Should fail to connect");
        } catch (JMSSecurityException ex) {
            LOG.info("Caught expected security error");
        }

        connection = pooledConnFact.createConnection("admin", "admin");
        connection.start();

        LOG.info("Successfully create new connection.");

        connection.close();
    }

    @Test
    public void testFailedConnectThenSucceedsWithListener() throws JMSException {
        Connection connection = pooledConnFact.createConnection("invalid", "credentials");
        connection.setExceptionListener(new ExceptionListener() {

            @Override
            public void onException(JMSException exception) {
                LOG.warn("Connection get error: {}", exception.getMessage());
            }
        });

        try {
            connection.start();
            fail("Should fail to connect");
        } catch (JMSSecurityException ex) {
            LOG.info("Caught expected security error");
        }

        connection = pooledConnFact.createConnection("admin", "admin");
        connection.start();

        LOG.info("Successfully create new connection.");

        connection.close();
    }

    @Test
    public void testFailureGetsNewConnectionOnRetryLooped() throws Exception {
        for (int i = 0; i < 10; ++i) {
            testFailureGetsNewConnectionOnRetry();
        }
    }

    @Test
    public void testFailureGetsNewConnectionOnRetry() throws Exception {
        pooledConnFact.setMaxConnections(1);

        final JmsPoolConnection connection1 = (JmsPoolConnection) pooledConnFact.createConnection("invalid", "credentials");

        try {
            connection1.start();
            fail("Should fail to connect");
        } catch (JMSSecurityException ex) {
            LOG.info("Caught expected security error");
        }

        // The pool should process the async error
        assertTrue("Should get new connection", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return connection1.getConnection() !=
                    ((JmsPoolConnection) pooledConnFact.createConnection("invalid", "credentials")).getConnection();
            }
        }));

        final JmsPoolConnection connection2 = (JmsPoolConnection) pooledConnFact.createConnection("invalid", "credentials");
        assertNotSame(connection1.getConnection(), connection2.getConnection());

        try {
            connection2.start();
            fail("Should fail to connect");
        } catch (JMSSecurityException ex) {
            LOG.info("Caught expected security error");
        } finally {
            connection2.close();
        }

        connection1.close();
    }

    @Test
    public void testFailureGetsNewConnectionOnRetryBigPool() throws JMSException {
        pooledConnFact.setMaxConnections(10);

        Connection connection1 = pooledConnFact.createConnection("invalid", "credentials");
        try {
            connection1.start();
            fail("Should fail to connect");
        } catch (JMSSecurityException ex) {
            LOG.info("Caught expected security error");
        }

        Connection connection2 = pooledConnFact.createConnection("invalid", "credentials");
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
    public void testFailoverWithInvalidCredentialsCanConnect() throws JMSException {
        pooledConnFact = createFailoverPooledConnectionFactory();
        pooledConnFact.setMaxConnections(1);

        Connection connection = pooledConnFact.createConnection("invalid", "credentials");

        try {
            connection.start();
            fail("Should fail to connect");
        } catch (JMSSecurityException ex) {
            LOG.info("Caught expected security error");
        }

        connection = pooledConnFact.createConnection("admin", "admin");
        connection.start();

        LOG.info("Successfully create new connection.");

        connection.close();
    }

    @Test
    public void testFailoverWithInvalidCredentials() throws Exception {
        pooledConnFact = createFailoverPooledConnectionFactory();
        pooledConnFact.setMaxConnections(1);

        final JmsPoolConnection connection1 = (JmsPoolConnection) pooledConnFact.createConnection("invalid", "credentials");

        try {
            connection1.start();
            fail("Should fail to connect");
        } catch (JMSSecurityException ex) {
            LOG.info("Caught expected security error");
            // Intentionally don't close here to see that async pool reconnect takes place.
        }

        // The pool should process the async error
        assertTrue("Should get new connection", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return connection1.getConnection() !=
                      ((JmsPoolConnection) pooledConnFact.createConnection("invalid", "credentials")).getConnection();
            }
        }));

        final JmsPoolConnection connection2 = (JmsPoolConnection) pooledConnFact.createConnection("invalid", "credentials");
        assertNotSame(connection1.getConnection(), connection2.getConnection());

        try {
            connection2.start();
            fail("Should fail to connect");
        } catch (JMSSecurityException ex) {
            LOG.info("Caught expected security error");
        } finally {
            connection2.close();
        }

        connection1.close();
    }

    @Test
    public void testFailedCreateConsumerConnectionStillWorks() throws JMSException {
        Connection connection = pooledConnFact.createConnection("guest", "password");
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());

        try {
            session.createConsumer(queue);
            fail("Should fail to create consumer");
        } catch (JMSSecurityException ex) {
            LOG.info("Caught expected security error");
        }

        queue = session.createQueue("GUESTS." + name.getMethodName());

        MessageProducer producer = session.createProducer(queue);
        producer.close();

        connection.close();
    }
}
