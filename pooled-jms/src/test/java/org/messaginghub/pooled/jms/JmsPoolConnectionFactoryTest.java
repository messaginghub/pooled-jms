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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.IllegalStateException;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.messaginghub.pooled.jms.mock.MockJMSConnection;
import org.messaginghub.pooled.jms.mock.MockJMSConnectionFactory;
import org.messaginghub.pooled.jms.util.Wait;

/**
 * Performs basic tests on the JmsPoolConnectionFactory implementation.
 */
@Timeout(60)
public class JmsPoolConnectionFactoryTest extends JmsPoolTestSupport {

    public final static Logger LOG = Logger.getLogger(JmsPoolConnectionFactoryTest.class);

    @Test
    public void testInstanceOf() throws  Exception {
        cf = new JmsPoolConnectionFactory();
        assertTrue(cf instanceof QueueConnectionFactory);
        assertTrue(cf instanceof TopicConnectionFactory);
        cf.stop();
    }

    @Test
    public void testSetTimeBetweenExpirationCheckMillis() throws  Exception {
        cf = new JmsPoolConnectionFactory();
        assertEquals(-1, cf.getConnectionCheckInterval());
        cf.setConnectionCheckInterval(5000);
        assertEquals(5000, cf.getConnectionCheckInterval());
    }

    @Test
    public void testGetConnectionFactory() throws  Exception {
        cf = new JmsPoolConnectionFactory();
        assertNull(cf.getConnectionFactory(), "Should not have any factory set yet");
        cf.setConnectionFactory(factory);
        assertNotNull(cf.getConnectionFactory(), "Should have a factory set yet");
        assertSame(factory, cf.getConnectionFactory());
    }

    @Test
    public void testFactoryRejectsNonConnectionFactorySet() throws  Exception {
        assertThrows(IllegalArgumentException.class, () -> cf.setConnectionFactory(""));
    }

    @Test
    public void testCreateConnectionWithNoFactorySet() throws  Exception {
        cf = new JmsPoolConnectionFactory();

        assertThrows(IllegalStateException.class, () -> cf.createConnection());
    }

    @Test
    public void testCreateConnection() throws Exception {
        Connection connection = cf.createConnection();

        assertNotNull(connection);
        assertEquals(1, cf.getNumConnections());

        connection.close();

        assertEquals(1, cf.getNumConnections());
    }

    @Test
    public void testCreateConnectionWithCredentials() throws Exception {
        Connection connection = cf.createConnection("user", "pass");

        assertNotNull(connection);
        assertEquals(1, cf.getNumConnections());

        connection.close();

        assertEquals(1, cf.getNumConnections());
    }

    @Test
    public void testQueueCreateConnection() throws Exception {
        QueueConnection connection = cf.createQueueConnection();

        assertNotNull(connection);
        assertEquals(1, cf.getNumConnections());

        connection.close();

        assertEquals(1, cf.getNumConnections());
    }

    @Test
    public void testQueueCreateConnectionWithCredentials() throws Exception {
        QueueConnection connection = cf.createQueueConnection("user", "pass");

        assertNotNull(connection);
        assertEquals(1, cf.getNumConnections());

        connection.close();

        assertEquals(1, cf.getNumConnections());
    }

    @Test
    public void testTopicCreateConnection() throws Exception {
        TopicConnection connection = cf.createTopicConnection();

        assertNotNull(connection);
        assertEquals(1, cf.getNumConnections());

        connection.close();

        assertEquals(1, cf.getNumConnections());
    }

    @Test
    public void testTopicCreateConnectionWithCredentials() throws Exception {
        TopicConnection connection = cf.createTopicConnection("user", "pass");

        assertNotNull(connection);
        assertEquals(1, cf.getNumConnections());

        connection.close();

        assertEquals(1, cf.getNumConnections());
    }

    @Test
    public void testClearAllConnections() throws Exception {
        MockJMSConnectionFactory mock = new MockJMSConnectionFactory();
        cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(mock);
        cf.setMaxConnections(3);

        JmsPoolConnection conn1 = (JmsPoolConnection) cf.createConnection();
        JmsPoolConnection conn2 = (JmsPoolConnection) cf.createConnection();
        JmsPoolConnection conn3 = (JmsPoolConnection) cf.createConnection();

        assertNotSame(conn1.getConnection(), conn2.getConnection());
        assertNotSame(conn1.getConnection(), conn3.getConnection());
        assertNotSame(conn2.getConnection(), conn3.getConnection());

        assertEquals(3, cf.getNumConnections());

        cf.clear();

        assertEquals(0, cf.getNumConnections());

        conn1 = (JmsPoolConnection) cf.createConnection();
        conn2 = (JmsPoolConnection) cf.createConnection();
        conn3 = (JmsPoolConnection) cf.createConnection();

        assertNotSame(conn1.getConnection(), conn2.getConnection());
        assertNotSame(conn1.getConnection(), conn3.getConnection());
        assertNotSame(conn2.getConnection(), conn3.getConnection());

        cf.stop();
    }

    @Test
    public void testClearDoesNotFailOnStoppedConnectionFactory() throws Exception {
        MockJMSConnectionFactory mock = new MockJMSConnectionFactory();
        cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(mock);
        cf.setMaxConnections(3);

        JmsPoolConnection conn1 = (JmsPoolConnection) cf.createConnection();
        JmsPoolConnection conn2 = (JmsPoolConnection) cf.createConnection();
        JmsPoolConnection conn3 = (JmsPoolConnection) cf.createConnection();

        assertNotSame(conn1.getConnection(), conn2.getConnection());
        assertNotSame(conn1.getConnection(), conn3.getConnection());
        assertNotSame(conn2.getConnection(), conn3.getConnection());

        assertEquals(3, cf.getNumConnections());

        cf.stop();

        assertEquals(0, cf.getNumConnections());

        try {
            cf.clear();
        } catch (Throwable error) {
            fail("Should not throw on clear of stopped factory.");
        }
    }

    @Test
    public void testMaxConnectionsAreCreated() throws Exception {
        MockJMSConnectionFactory mock = new MockJMSConnectionFactory();
        cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(mock);
        cf.setMaxConnections(3);

        JmsPoolConnection conn1 = (JmsPoolConnection) cf.createConnection();
        JmsPoolConnection conn2 = (JmsPoolConnection) cf.createConnection();
        JmsPoolConnection conn3 = (JmsPoolConnection) cf.createConnection();

        assertNotSame(conn1.getConnection(), conn2.getConnection());
        assertNotSame(conn1.getConnection(), conn3.getConnection());
        assertNotSame(conn2.getConnection(), conn3.getConnection());

        assertEquals(3, cf.getNumConnections());

        cf.stop();
    }

    @Test
    public void testCannotCreateConnectionOnStoppedFactory() throws Exception {
        MockJMSConnectionFactory mock = new MockJMSConnectionFactory();
        cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(mock);
        cf.setMaxConnections(100);
        cf.stop();

        assertEquals(0, cf.getNumConnections());
        assertNull(cf.createConnection());
        assertEquals(0, cf.getNumConnections());

        cf.start();

        assertNotNull(cf.createConnection());
        assertEquals(1, cf.getNumConnections());

        cf.stop();
    }

    @Test
    public void testCannotCreateContextOnStoppedFactory() throws Exception {
        MockJMSConnectionFactory mock = new MockJMSConnectionFactory();
        cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(mock);
        cf.setMaxConnections(100);
        cf.stop();

        assertEquals(0, cf.getNumConnections());
        assertNull(cf.createContext());
        assertEquals(0, cf.getNumConnections());

        cf.start();

        assertNotNull(cf.createContext());
        assertEquals(1, cf.getNumConnections());

        cf.stop();
    }

    @Test
    public void testFactoryStopStart() throws Exception {
        MockJMSConnectionFactory mock = new MockJMSConnectionFactory();
        cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(mock);
        cf.setMaxConnections(1);

        JmsPoolConnection conn1 = (JmsPoolConnection) cf.createConnection();

        cf.stop();

        assertNull(cf.createConnection());

        cf.start();

        JmsPoolConnection conn2 = (JmsPoolConnection) cf.createConnection();

        assertNotSame(conn1.getConnection(), conn2.getConnection());

        assertEquals(1, cf.getNumConnections());

        cf.stop();
    }

    @Test
    public void testConnectionsAreRotated() throws Exception {
        MockJMSConnectionFactory mock = new MockJMSConnectionFactory();
        cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(mock);
        cf.setMaxConnections(10);

        Connection previous = null;

        // Front load the pool.
        for (int i = 0; i < 10; ++i) {
            cf.createConnection();
        }

        for (int i = 0; i < 100; ++i) {
            Connection current = ((JmsPoolConnection) cf.createConnection()).getConnection();
            assertNotSame(previous, current);
            previous = current;
        }

        cf.stop();
    }

    @Test
    public void testConnectionsArePooled() throws Exception {
        MockJMSConnectionFactory mock = new MockJMSConnectionFactory();
        cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(mock);
        cf.setMaxConnections(1);

        JmsPoolConnection conn1 = (JmsPoolConnection) cf.createConnection();
        JmsPoolConnection conn2 = (JmsPoolConnection) cf.createConnection();
        JmsPoolConnection conn3 = (JmsPoolConnection) cf.createConnection();

        assertSame(conn1.getConnection(), conn2.getConnection());
        assertSame(conn1.getConnection(), conn3.getConnection());
        assertSame(conn2.getConnection(), conn3.getConnection());

        assertEquals(1, cf.getNumConnections());

        cf.stop();
    }

    @Test
    public void testConnectionsArePooledAsyncCreate() throws Exception {
        final MockJMSConnectionFactory mock = new MockJMSConnectionFactory();

        cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(mock);
        cf.setMaxConnections(1);

        final ConcurrentLinkedQueue<JmsPoolConnection> connections = new ConcurrentLinkedQueue<JmsPoolConnection>();

        final JmsPoolConnection primary = (JmsPoolConnection) cf.createConnection();
        final ExecutorService executor = Executors.newFixedThreadPool(10);
        final int numConnections = 100;

        for (int i = 0; i < numConnections; ++i) {
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        connections.add((JmsPoolConnection) cf.createConnection());
                    } catch (JMSException e) {
                    }
                }
            });
        }

        assertTrue(Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
                return connections.size() == numConnections;
            }
        }, TimeUnit.SECONDS.toMillis(10), TimeUnit.MILLISECONDS.toMillis(50)), "All connections should have been created.");

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        for (JmsPoolConnection connection : connections) {
            assertSame(primary.getConnection(), connection.getConnection());
        }

        connections.clear();
        cf.stop();
    }

    @Test
    public void testConcurrentCreateGetsUniqueConnectionCreateOnDemand() throws Exception {
        doTestConcurrentCreateGetsUniqueConnection(false);
    }

    @Test
    public void testConcurrentCreateGetsUniqueConnectionCreateOnStart() throws Exception {
        doTestConcurrentCreateGetsUniqueConnection(true);
    }

    private void doTestConcurrentCreateGetsUniqueConnection(boolean createOnStart) throws Exception {
        final int numConnections = 2;

        final MockJMSConnectionFactory mock = new MockJMSConnectionFactory();
        cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(mock);
        cf.setMaxConnections(numConnections);
        cf.start();

        final ConcurrentMap<UUID, Connection> connections = new ConcurrentHashMap<>();
        final ExecutorService executor = Executors.newFixedThreadPool(numConnections);

        for (int i = 0; i < numConnections; ++i) {
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        JmsPoolConnection pooled = (JmsPoolConnection) cf.createConnection();
                        MockJMSConnection wrapped = (MockJMSConnection) pooled.getConnection();
                        connections.put(wrapped.getConnectionId(), pooled);
                    } catch (JMSException e) {
                    }
                }
            });
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));

        assertEquals(numConnections, connections.size(), "Should have all unique connections");

        connections.clear();
        cf.stop();
    }

    @Test
    public void testPooledBaseHandlesSubclassesInjectingInvalidFactoriesForConnection() throws Exception {
        cf = new BadFactoryJmsPoolConnectionFactory();
        cf.setConnectionFactory(UUID.randomUUID());

        try {
            cf.createConnection();
            fail("Should throw IllegalStateException when factory is an invalid type");
        } catch (IllegalStateException ise) {}
    }

    @Test
    public void testPooledBaseHandlesSubclassesInjectingInvalidFactoriesForContext() throws Exception {
        cf = new BadFactoryJmsPoolConnectionFactory();
        cf.setConnectionFactory(UUID.randomUUID());

        try {
            cf.createContext();
            fail("Should throw IllegalStateRuntimeException when factory is an invalid type");
        } catch (IllegalStateRuntimeException isre) {}
    }

    private class BadFactoryJmsPoolConnectionFactory extends JmsPoolConnectionFactory {

        @Override
        public void setConnectionFactory(Object factory) {
            // Simulate bad Pooled factory subclass to ensure we validate what it gave us.
            this.connectionFactory = factory;
            this.jmsContextSupported = true;
        }
    }

    @Test
    public void testPooledCreateContextFailsWhenJMS20NotSupported() throws Exception {
        cf = new JMS20NotAllowedJmsPoolConnectionFactory();
        cf.setConnectionFactory(new MockJMSConnectionFactory());

        try {
            cf.createContext();
            fail("Should throw JMSRuntimeException when told JMS 2.0 isn't available");
        } catch (JMSRuntimeException jmsre) {}
    }

    private class JMS20NotAllowedJmsPoolConnectionFactory extends JmsPoolConnectionFactory {

        @Override
        public void setConnectionFactory(Object factory) {
            super.setConnectionFactory(factory);
            this.jmsContextSupported = false;
        }
    }
}
