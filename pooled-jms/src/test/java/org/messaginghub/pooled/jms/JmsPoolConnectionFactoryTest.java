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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.messaginghub.pooled.jms.JmsPoolConnection;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;
import org.messaginghub.pooled.jms.mock.MockJMSConnection;
import org.messaginghub.pooled.jms.mock.MockJMSConnectionFactory;
import org.messaginghub.pooled.jms.util.Wait;

/**
 * Performs basic tests on the JmsPoolConnectionFactory implementation.
 */
public class JmsPoolConnectionFactoryTest extends JmsPoolTestSupport {

    public final static Logger LOG = Logger.getLogger(JmsPoolConnectionFactoryTest.class);

    @Test(timeout = 60000)
    public void testInstanceOf() throws  Exception {
        cf = new JmsPoolConnectionFactory();
        assertTrue(cf instanceof QueueConnectionFactory);
        assertTrue(cf instanceof TopicConnectionFactory);
        cf.stop();
    }

    @Test(timeout = 60000)
    public void testSetReconnectOnException() throws  Exception {
        cf = new JmsPoolConnectionFactory();
        assertTrue(cf.isReconnectOnException());
        cf.setReconnectOnException(false);
        assertFalse(cf.isReconnectOnException());
    }

    @Test(timeout = 60000)
    public void testSetTimeBetweenExpirationCheckMillis() throws  Exception {
        cf = new JmsPoolConnectionFactory();
        assertEquals(-1, cf.getTimeBetweenExpirationCheckMillis());
        cf.setTimeBetweenExpirationCheckMillis(5000);
        assertEquals(5000, cf.getTimeBetweenExpirationCheckMillis());
    }

    @Test(timeout = 60000)
    public void testGetConnectionFactory() throws  Exception {
        cf = new JmsPoolConnectionFactory();
        assertNull("Should not have any factory set yet", cf.getConnectionFactory());
        cf.setConnectionFactory(factory);
        assertNotNull("Should have a factory set yet", cf.getConnectionFactory());
        assertSame(factory, cf.getConnectionFactory());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFactoryRejectsNonConnectionFactorySet() throws  Exception {
        cf.setConnectionFactory("");
    }

    @Test(expected = IllegalStateException.class)
    public void testCreateConnectionWithNoFactorySet() throws  Exception {
        cf = new JmsPoolConnectionFactory();
        cf.createConnection();
    }

    @Test(timeout = 60000)
    public void testCreateConnection() throws Exception {
        Connection connection = cf.createConnection();

        assertNotNull(connection);
        assertEquals(1, cf.getNumConnections());

        connection.close();

        assertEquals(1, cf.getNumConnections());
    }

    @Test(timeout = 60000)
    public void testCreateConnectionWithCredentials() throws Exception {
        Connection connection = cf.createConnection("user", "pass");

        assertNotNull(connection);
        assertEquals(1, cf.getNumConnections());

        connection.close();

        assertEquals(1, cf.getNumConnections());
    }

    @Test(timeout = 60000)
    public void testQueueCreateConnection() throws Exception {
        QueueConnection connection = cf.createQueueConnection();

        assertNotNull(connection);
        assertEquals(1, cf.getNumConnections());

        connection.close();

        assertEquals(1, cf.getNumConnections());
    }

    @Test(timeout = 60000)
    public void testQueueCreateConnectionWithCredentials() throws Exception {
        QueueConnection connection = cf.createQueueConnection("user", "pass");

        assertNotNull(connection);
        assertEquals(1, cf.getNumConnections());

        connection.close();

        assertEquals(1, cf.getNumConnections());
    }

    @Test(timeout = 60000)
    public void testTopicCreateConnection() throws Exception {
        TopicConnection connection = cf.createTopicConnection();

        assertNotNull(connection);
        assertEquals(1, cf.getNumConnections());

        connection.close();

        assertEquals(1, cf.getNumConnections());
    }

    @Test(timeout = 60000)
    public void testTopicCreateConnectionWithCredentials() throws Exception {
        TopicConnection connection = cf.createTopicConnection("user", "pass");

        assertNotNull(connection);
        assertEquals(1, cf.getNumConnections());

        connection.close();

        assertEquals(1, cf.getNumConnections());
    }

    @Test(timeout = 60000)
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

    @Test(timeout = 60000)
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

    @Test(timeout = 60000)
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

    @Test(timeout = 60000)
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

    @Test(timeout = 60000)
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

    @Test(timeout = 60000)
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

    @Test(timeout = 60000)
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

    @Test(timeout = 60000)
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

    @Test(timeout = 60000)
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

        assertTrue("All connections should have been created.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
                return connections.size() == numConnections;
            }
        }, TimeUnit.SECONDS.toMillis(10), TimeUnit.MILLISECONDS.toMillis(50)));

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        for (JmsPoolConnection connection : connections) {
            assertSame(primary.getConnection(), connection.getConnection());
        }

        connections.clear();
        cf.stop();
    }

    @Test(timeout = 60000)
    public void testConcurrentCreateGetsUniqueConnectionCreateOnDemand() throws Exception {
        doTestConcurrentCreateGetsUniqueConnection(false);
    }

    @Test(timeout = 60000)
    public void testConcurrentCreateGetsUniqueConnectionCreateOnStart() throws Exception {
        doTestConcurrentCreateGetsUniqueConnection(true);
    }

    private void doTestConcurrentCreateGetsUniqueConnection(boolean createOnStart) throws Exception {
        final int numConnections = 2;

        final MockJMSConnectionFactory mock = new MockJMSConnectionFactory();
        cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(mock);
        cf.setMaxConnections(numConnections);
        cf.setCreateConnectionOnStartup(createOnStart);
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

        assertEquals("Should have all unique connections", numConnections, connections.size());

        connections.clear();
        cf.stop();
    }
}
