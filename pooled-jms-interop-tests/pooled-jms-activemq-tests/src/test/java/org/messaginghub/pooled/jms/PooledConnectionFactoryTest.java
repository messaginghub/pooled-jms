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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.QueueConnectionFactory;
import javax.jms.Session;
import javax.jms.TopicConnectionFactory;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.command.ConnectionId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.messaginghub.pooled.jms.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks the behavior of the PooledConnectionFactory when the maximum amount of
 * sessions is being reached.
 *
 * Older versions simply block in the call to Connection.getSession(), which
 * isn't good. An exception being returned is the better option, so JMS clients
 * don't block. This test succeeds if an exception is returned and fails if the
 * call to getSession() blocks.
 */
@Timeout(60)
public class PooledConnectionFactoryTest extends ActiveMQJmsPoolTestSupport {

    public final static Logger LOG = LoggerFactory.getLogger(PooledConnectionFactoryTest.class);

    @Test
    public void testInstanceOf() throws  Exception {
        JmsPoolConnectionFactory pcf = new JmsPoolConnectionFactory();
        assertTrue(pcf instanceof QueueConnectionFactory);
        assertTrue(pcf instanceof TopicConnectionFactory);
        pcf.stop();
    }

    @Test
    public void testFailToCreateJMSContext() throws  Exception {
        JmsPoolConnectionFactory cf = createPooledConnectionFactory();

        try {
            cf.createContext();
            fail("Should have thrown a JMSRuntimeException");
        } catch (JMSRuntimeException jmsre) {
            LOG.info("Caught Excepted JMSRuntimeException");
        } finally {
            cf.stop();
        }
    }

    @Test
    public void testFailToCreateJMSContextWithSessionMode() throws  Exception {
        JmsPoolConnectionFactory cf = createPooledConnectionFactory();

        try {
            cf.createContext(Session.SESSION_TRANSACTED);
            fail("Should have thrown a JMSRuntimeException");
        } catch (JMSRuntimeException jmsre) {
            LOG.info("Caught Excepted JMSRuntimeException");
        } finally {
            cf.stop();
        }
    }

    @Test
    public void testFailToCreateJMSContextWithCredentials() throws  Exception {
        JmsPoolConnectionFactory cf = createPooledConnectionFactory();

        try {
            cf.createContext("user", "pass");
            fail("Should have thrown a JMSRuntimeException");
        } catch (JMSRuntimeException jmsre) {
            LOG.info("Caught Excepted JMSRuntimeException");
        } finally {
            cf.stop();
        }
    }

    @Test
    public void testFailToCreateJMSContextWithCredentialsAndSessionMode() throws  Exception {
        JmsPoolConnectionFactory cf = createPooledConnectionFactory();

        try {
            cf.createContext("user", "pass", Session.CLIENT_ACKNOWLEDGE);
            fail("Should have thrown a JMSRuntimeException");
        } catch (JMSRuntimeException jmsre) {
            LOG.info("Caught Excepted JMSRuntimeException");
        } finally {
            cf.stop();
        }
    }

    @Test
    public void testClearAllConnections() throws Exception {
        JmsPoolConnectionFactory cf = createPooledConnectionFactory();
        cf.setMaxConnections(3);

        try {
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
        } finally {
            cf.stop();
        }
    }

    @Test
    public void testMaxConnectionsAreCreated() throws Exception {
        JmsPoolConnectionFactory cf = createPooledConnectionFactory();
        cf.setMaxConnections(3);

        try {
            JmsPoolConnection conn1 = (JmsPoolConnection) cf.createConnection();
            JmsPoolConnection conn2 = (JmsPoolConnection) cf.createConnection();
            JmsPoolConnection conn3 = (JmsPoolConnection) cf.createConnection();

            assertNotSame(conn1.getConnection(), conn2.getConnection());
            assertNotSame(conn1.getConnection(), conn3.getConnection());
            assertNotSame(conn2.getConnection(), conn3.getConnection());

            assertEquals(3, cf.getNumConnections());
        } finally {
            cf.stop();
        }
    }

    @Test
    public void testFactoryStopStart() throws Exception {
        JmsPoolConnectionFactory cf = createPooledConnectionFactory();
        cf.setMaxConnections(1);

        try {
            JmsPoolConnection conn1 = (JmsPoolConnection) cf.createConnection();

            cf.stop();

            assertNull(cf.createConnection());

            cf.start();

            JmsPoolConnection conn2 = (JmsPoolConnection) cf.createConnection();

            assertNotSame(conn1.getConnection(), conn2.getConnection());

            assertEquals(1, cf.getNumConnections());
        } finally {
            cf.stop();
        }
    }

    @Test
    public void testConnectionsAreRotated() throws Exception {
        JmsPoolConnectionFactory cf = createPooledConnectionFactory();
        cf.setMaxConnections(10);

        try {
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
        } finally {
            cf.stop();
        }
    }

    @Test
    public void testConnectionsArePooled() throws Exception {
        JmsPoolConnectionFactory cf = createPooledConnectionFactory();
        cf.setMaxConnections(1);

        try {
            JmsPoolConnection conn1 = (JmsPoolConnection) cf.createConnection();
            JmsPoolConnection conn2 = (JmsPoolConnection) cf.createConnection();
            JmsPoolConnection conn3 = (JmsPoolConnection) cf.createConnection();

            assertSame(conn1.getConnection(), conn2.getConnection());
            assertSame(conn1.getConnection(), conn3.getConnection());
            assertSame(conn2.getConnection(), conn3.getConnection());

            assertEquals(1, cf.getNumConnections());
        } finally {
            cf.stop();
        }
    }

    @Test
    public void testConnectionsArePooledAsyncCreate() throws Exception {
        final JmsPoolConnectionFactory cf = createPooledConnectionFactory();
        cf.setMaxConnections(1);

        final ConcurrentLinkedQueue<JmsPoolConnection> connections = new ConcurrentLinkedQueue<JmsPoolConnection>();

        try {
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
        } finally {
            cf.stop();
        }
    }

    @Test
    public void testConcurrentCreateGetsUniqueConnection() throws Exception {
        final JmsPoolConnectionFactory cf = createPooledConnectionFactory();

        try {
            final int numConnections = 2;

            cf.setMaxConnections(numConnections);
            cf.start();

            final ConcurrentMap<ConnectionId, Connection> connections = new ConcurrentHashMap<ConnectionId, Connection>();
            final ExecutorService executor = Executors.newFixedThreadPool(numConnections);

            for (int i = 0; i < numConnections; ++i) {
                executor.execute(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            JmsPoolConnection pooled = (JmsPoolConnection) cf.createConnection();
                            ActiveMQConnection amq = (ActiveMQConnection) pooled.getConnection();
                            connections.put(amq.getConnectionInfo().getConnectionId(), pooled);
                        } catch (JMSException e) {
                        }
                    }
                });
            }

            executor.shutdown();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));

            assertEquals(numConnections, connections.size(), "Should have all unique connections");

            connections.clear();
        } finally {
            cf.stop();
        }
    }
}
