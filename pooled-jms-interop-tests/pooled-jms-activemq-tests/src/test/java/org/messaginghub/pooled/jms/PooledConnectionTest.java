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
import static org.junit.jupiter.api.Assertions.fail;

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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.messaginghub.pooled.jms.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests against the PooledConnection class.
 */
@Timeout(60)
public class PooledConnectionTest extends ActiveMQJmsPoolTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(PooledConnectionTest.class);

    @Test
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

    @Test
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
            cf.stop();
        }

        LOG.debug("Test finished.");
    }

    @Test
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
    @Test
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

    private class TestRunner implements Callable<Boolean> {

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
                cf = createPooledConnectionFactory();
                cf.setMaxConnections(3);
                cf.setMaxSessionsPerConnection(1);
                cf.setBlockIfSessionPoolIsFull(false);

                conn = cf.createConnection();
                one = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

                Session two = null;
                try {
                    // this should raise an exception as we called setMaximumActive(1)
                    two = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    two.close();

                    LOG.error("Expected JMSException wasn't thrown.");
                    fail("seconds call to Connection.createSession() was supposed" +
                         "to raise an JMSException as internal session pool" +
                         "is exhausted. This did not happen and indicates a problem");
                    return Boolean.FALSE;
                } catch (JMSException ex) {
                    if (ex.getCause().getClass() == java.util.NoSuchElementException.class) {
                        // expected, ignore but log
                        LOG.info("Caught expected " + ex);
                    } else {
                        LOG.error("Error trapped", ex);
                        return Boolean.FALSE;
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
                LOG.error(ex.getMessage());
                return Boolean.FALSE;
            } finally {
                if (cf != null) {
                    cf.stop();
                }
            }

            // all good, test succeeded
            return Boolean.TRUE;
        }
    }

    @Test
    public void testAllSessionsAvailableOnConstrainedPool() throws Exception {
        JmsPoolConnectionFactory cf = createPooledConnectionFactory();
        cf.setMaxConnections(5);
        cf.setMaxSessionsPerConnection(2);
        cf.setBlockIfSessionPoolIsFull(false);

        LinkedList<Connection> connections = new LinkedList<>();
        HashSet<Session> sessions = new HashSet<>();

        Connection connection = null;

        try {
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
        } finally {
            if (cf != null) {
                cf.stop();
            }
        }
    }
}

