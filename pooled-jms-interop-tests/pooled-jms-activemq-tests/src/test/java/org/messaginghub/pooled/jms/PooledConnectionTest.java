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
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;
import org.messaginghub.pooled.jms.JmsPoolConnection;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;
import org.messaginghub.pooled.jms.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests against the PooledConnection class.
 */
public class PooledConnectionTest extends ActiveMQJmsPoolTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(PooledConnectionTest.class);

    @Test(timeout = 60000)
    public void testSetClientIDTwiceWithSameID() throws Exception {
        LOG.debug("running testRepeatedSetClientIDCalls()");

        // test: call setClientID("newID") twice
        // this should be tolerated and not result in an exception
        JmsPoolConnectionFactory cf = createPooledConnectionFactory();
        Connection conn = cf.createConnection();
        conn.setClientID("newID");

        try {
            conn.setClientID("newID");
            conn.start();
            conn.close();
        } catch (IllegalStateException ise) {
            LOG.error("Repeated calls to newID2.setClientID(\"newID\") caused " + ise.getMessage());
            fail("Repeated calls to newID2.setClientID(\"newID\") caused " + ise.getMessage());
        } finally {
            cf.stop();
        }

        LOG.debug("Test finished.");
    }

    @Test(timeout = 60000)
    public void testSetClientIDTwiceWithDifferentID() throws Exception {
        LOG.debug("running testRepeatedSetClientIDCalls()");

        JmsPoolConnectionFactory cf = createPooledConnectionFactory();
        Connection conn = cf.createConnection();

        // test: call setClientID() twice with different IDs
        // this should result in an IllegalStateException
        conn.setClientID("newID1");
        try {
            conn.setClientID("newID2");
            fail("calling Connection.setClientID() twice with different clientID must raise an IllegalStateException");
        } catch (IllegalStateException ise) {
            LOG.debug("Correctly received " + ise);
        } finally {
            conn.close();
            cf.stop();
        }

        LOG.debug("Test finished.");
    }

    @Test(timeout = 60000)
    public void testSetClientIDAfterConnectionStart() throws Exception {
        LOG.debug("running testRepeatedSetClientIDCalls()");

        JmsPoolConnectionFactory cf = createPooledConnectionFactory();
        Connection conn = cf.createConnection();

        // test: try to call setClientID() after start()
        // should result in an exception
        try {
            conn.start();
            conn.setClientID("newID3");
            fail("Calling setClientID() after start() mut raise a JMSException.");
        } catch (IllegalStateException ise) {
            LOG.debug("Correctly received " + ise);
        } finally {
            conn.close();
            cf.stop();
        }

        LOG.debug("Test finished.");
    }

    /**
     * Tests the behavior of the sessionPool of the PooledConnectionFactory when
     * maximum number of sessions are reached.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testCreateSessionDoesNotBlockWhenNotConfiguredTo() throws Exception {
        // using separate thread for testing so that we can interrupt the test
        // if the call to get a new session blocks.

        // start test runner thread
        ExecutorService executor = Executors.newSingleThreadExecutor();
        final Future<Boolean> result = executor.submit(new TestRunner());

        boolean testPassed = Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return result.isDone() && result.get().booleanValue();
            }
        }, TimeUnit.SECONDS.toMillis(10), TimeUnit.MILLISECONDS.toMillis(50));

        if (!testPassed) {
            PooledConnectionFactoryTest.LOG.error("2nd call to createSession() " +
                                                  "is blocking but should have returned an error instead.");
            executor.shutdownNow();
            fail("SessionPool inside PooledConnectionFactory is blocking if " +
                 "limit is exceeded but should return an exception instead.");
        }
    }

    static class TestRunner implements Callable<Boolean> {

        private static final Logger TASK_LOG = LoggerFactory.getLogger(PooledConnectionTest.class);

        /**
         * @return true if test succeeded, false otherwise
         */
        @Override
        public Boolean call() {

            Connection conn = null;
            Session one = null;

            JmsPoolConnectionFactory cf = null;

            // wait at most 5 seconds for the call to createSession
            try {
                ActiveMQConnectionFactory amq = new ActiveMQConnectionFactory(
                    "vm://broker1?marshal=false&broker.persistent=false&broker.useJmx=false");
                cf = new JmsPoolConnectionFactory();
                cf.setConnectionFactory(amq);
                cf.setMaxConnections(3);
                cf.setMaximumActiveSessionPerConnection(1);
                cf.setBlockIfSessionPoolIsFull(false);

                conn = cf.createConnection();
                one = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

                Session two = null;
                try {
                    // this should raise an exception as we called setMaximumActive(1)
                    two = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    two.close();

                    TASK_LOG.error("Expected JMSException wasn't thrown.");
                    fail("seconds call to Connection.createSession() was supposed" +
                         "to raise an JMSException as internal session pool" +
                         "is exhausted. This did not happen and indiates a problem");
                    return new Boolean(false);
                } catch (JMSException ex) {
                    if (ex.getCause().getClass() == java.util.NoSuchElementException.class) {
                        // expected, ignore but log
                        TASK_LOG.info("Caught expected " + ex);
                    } else {
                        TASK_LOG.error("Error trapped", ex);
                        return new Boolean(false);
                    }
                } finally {
                    if (one != null) {
                        one.close();
                    }
                    if (conn != null) {
                        conn.close();
                    }
                }
            } catch (Exception ex) {
                TASK_LOG.error(ex.getMessage());
                return new Boolean(false);
            } finally {
                if (cf != null) {
                    cf.stop();
                }
            }

            // all good, test succeeded
            return new Boolean(true);
        }
    }

    @Test(timeout = 60000)
    public void testAllSessionsAvailableOnConstrainedPool() throws Exception {
        JmsPoolConnectionFactory cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(new ActiveMQConnectionFactory(
                "vm://localhost?broker.persistent=false&broker.useJmx=false&broker.schedulerSupport=false"));
        cf.setMaxConnections(5);
        cf.setMaximumActiveSessionPerConnection(2);
        cf.setBlockIfSessionPoolIsFull(false);

        LinkedList<Connection> connections = new LinkedList<>();
        HashSet<Session> sessions = new HashSet<>();

        Connection connection = null;

        for (int i = 0; i < 10; i++) {
            connection = cf.createConnection();
            LOG.info("connection: " + i + ", " + ((JmsPoolConnection) connection).getConnection());

            connection.start();
            connections.add(connection);
            sessions.add(connection.createSession(false, Session.AUTO_ACKNOWLEDGE));
        }

        assertEquals(sessions.size(), 10);
        assertEquals(connections.size(), 10);

        Connection connectionToClose = connections.getLast();
        connectionToClose.close();

        connection = cf.createConnection();
        LOG.info("connection:" + ((JmsPoolConnection) connection).getConnection());

        connection.start();
        connections.add(connection);
        try {
            sessions.add(connection.createSession(false, Session.AUTO_ACKNOWLEDGE));
        } catch (JMSException expected) {
            connection.close();
        }

        connection = cf.createConnection();
        LOG.info("connection:" + ((JmsPoolConnection) connection).getConnection());

        connection.start();
        connections.add(connection);
        try {
            sessions.add(connection.createSession(false, Session.AUTO_ACKNOWLEDGE));
        } catch (JMSException expected) {
            connection.close();
        }

        assertEquals(sessions.size(), 10);
        assertEquals(connections.size(), 12);
    }
}
