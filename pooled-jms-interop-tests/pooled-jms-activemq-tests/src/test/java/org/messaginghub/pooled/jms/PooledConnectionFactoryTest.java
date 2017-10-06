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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ConnectionId;
import org.junit.Test;
import org.messaginghub.pooled.jms.JmsPoolConnection;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;
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
public class PooledConnectionFactoryTest extends ActiveMQJmsPoolTestSupport {

    public final static Logger LOG = LoggerFactory.getLogger(PooledConnectionFactoryTest.class);

    @Test(timeout = 60000)
    public void testInstanceOf() throws  Exception {
        JmsPoolConnectionFactory pcf = new JmsPoolConnectionFactory();
        assertTrue(pcf instanceof QueueConnectionFactory);
        assertTrue(pcf instanceof TopicConnectionFactory);
        pcf.stop();
    }

    @Test(timeout = 60000)
    public void testFailToCreateJMSContext() throws  Exception {
        JmsPoolConnectionFactory cf = createPooledConnectionFactory();

        try {
            cf.createContext();
            fail("Should have thrown a JMSRuntimeException");
        } catch (JMSRuntimeException jmsre) {
            LOG.info("Caught Excepted JMSRuntimeException");
        }
    }

    @Test(timeout = 60000)
    public void testFailToCreateJMSContextWithSessionMode() throws  Exception {
        JmsPoolConnectionFactory cf = createPooledConnectionFactory();

        try {
            cf.createContext(Session.SESSION_TRANSACTED);
            fail("Should have thrown a JMSRuntimeException");
        } catch (JMSRuntimeException jmsre) {
            LOG.info("Caught Excepted JMSRuntimeException");
        }
    }

    @Test(timeout = 60000)
    public void testFailToCreateJMSContextWithCredentials() throws  Exception {
        JmsPoolConnectionFactory cf = createPooledConnectionFactory();

        try {
            cf.createContext("user", "pass");
            fail("Should have thrown a JMSRuntimeException");
        } catch (JMSRuntimeException jmsre) {
            LOG.info("Caught Excepted JMSRuntimeException");
        }
    }

    @Test(timeout = 60000)
    public void testFailToCreateJMSContextWithCredentialsAndSessionMode() throws  Exception {
        JmsPoolConnectionFactory cf = createPooledConnectionFactory();

        try {
            cf.createContext("user", "pass", Session.CLIENT_ACKNOWLEDGE);
            fail("Should have thrown a JMSRuntimeException");
        } catch (JMSRuntimeException jmsre) {
            LOG.info("Caught Excepted JMSRuntimeException");
        }
    }

    @Test(timeout = 60000)
    public void testClearAllConnections() throws Exception {

        ActiveMQConnectionFactory amq = new ActiveMQConnectionFactory(
            "vm://broker1?marshal=false&broker.persistent=false&broker.useJmx=false");
        JmsPoolConnectionFactory cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(amq);
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
    public void testMaxConnectionsAreCreated() throws Exception {

        ActiveMQConnectionFactory amq = new ActiveMQConnectionFactory(
            "vm://broker1?marshal=false&broker.persistent=false&broker.useJmx=false");
        JmsPoolConnectionFactory cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(amq);
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
    public void testFactoryStopStart() throws Exception {

        ActiveMQConnectionFactory amq = new ActiveMQConnectionFactory(
            "vm://broker1?marshal=false&broker.persistent=false&broker.useJmx=false");
        JmsPoolConnectionFactory cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(amq);
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

        ActiveMQConnectionFactory amq = new ActiveMQConnectionFactory(
            "vm://broker1?marshal=false&broker.persistent=false&broker.useJmx=false");
        JmsPoolConnectionFactory cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(amq);
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

        ActiveMQConnectionFactory amq = new ActiveMQConnectionFactory("vm://broker1?marshal=false&broker.persistent=false");
        JmsPoolConnectionFactory cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(amq);
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

        final ActiveMQConnectionFactory amq = new ActiveMQConnectionFactory(
            "vm://broker1?marshal=false&broker.persistent=false&broker.useJmx=false");
        final JmsPoolConnectionFactory cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(amq);
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

        BrokerService brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.addConnector("tcp://localhost:0");
        brokerService.start();
        brokerService.waitUntilStarted();

        try {
            final int numConnections = 2;

            final ActiveMQConnectionFactory amq = new ActiveMQConnectionFactory(brokerService.getTransportConnectors().get(0).getPublishableConnectString());
            final JmsPoolConnectionFactory cf = new JmsPoolConnectionFactory();
            cf.setConnectionFactory(amq);
            cf.setMaxConnections(numConnections);
            cf.setCreateConnectionOnStartup(createOnStart);
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

            assertEquals("Should have all unique connections", numConnections, connections.size());

            connections.clear();
            cf.stop();

        } finally {
            brokerService.stop();
        }
    }
}
