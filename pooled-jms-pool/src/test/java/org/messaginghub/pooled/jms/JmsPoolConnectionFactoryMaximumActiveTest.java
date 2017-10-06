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
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

import org.junit.Before;
import org.junit.Test;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;
import org.messaginghub.pooled.jms.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks the behavior of the PooledConnectionFactory when the maximum amount of sessions is being reached
 * (maximumActive). When using setBlockIfSessionPoolIsFull(true) on the ConnectionFactory, further requests for sessions
 * should block. If it does not block, its a bug.
 */
public class JmsPoolConnectionFactoryMaximumActiveTest extends JmsPoolTestSupport {

    public final static Logger LOG = LoggerFactory.getLogger(JmsPoolConnectionFactoryMaximumActiveTest.class);

    private static Connection connection = null;
    private static ConcurrentMap<Integer, Session> sessions = new ConcurrentHashMap<Integer, Session>();

    public static void addSession(Session s) {
        sessions.put(s.hashCode(), s);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        sessions.clear();
    }

    @Test(timeout = 60000)
    public void testCreateSessionBlocksWhenMaxSessionsLoanedOutUntilReturned() throws Exception {
        cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(factory);
        cf.setMaxConnections(3);
        cf.setMaximumActiveSessionPerConnection(1);
        cf.setBlockIfSessionPoolIsFull(true);

        connection = cf.createConnection();

        // start test runner threads. It is expected that the second thread
        // blocks on the call to createSession()

        ExecutorService executor = Executors.newFixedThreadPool(1);
        Future<Boolean> result1 = executor.submit(new SessionTakerAndReturner());
        Future<Boolean> result2 = executor.submit(new SessionTaker());

        assertTrue(Wait.waitFor(() -> { return result1.isDone(); }, 5000, 10));
        assertTrue(Wait.waitFor(() -> { return result2.isDone(); }, 5000, 10));

        // Two sessions should have been returned
        assertEquals(2, sessions.size());

        // Take all threads down
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    /**
     * Tests the behavior of the sessionPool of the PooledConnectionFactory when maximum number of
     * sessions are reached.  This test uses maximumActive=1. When creating two threads that both try
     * to create a JMS session from the same JMS connection, the thread that is second to call
     * createSession() should block (as only 1 session is allowed) until the session is returned to
     * pool. If it does not block, its a bug.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testCreateSessionBlocksWhenMaxSessionsLoanedOut() throws Exception {
        cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(factory);
        cf.setMaxConnections(3);
        cf.setMaximumActiveSessionPerConnection(1);
        cf.setBlockIfSessionPoolIsFull(true);

        connection = cf.createConnection();

        // start test runner threads. It is expected that the second thread
        // blocks on the call to createSession()

        ExecutorService executor = Executors.newFixedThreadPool(1);
        Future<Boolean> result1 = executor.submit(new SessionTaker());
        Future<Boolean> result2 = executor.submit(new SessionTaker());

        assertTrue(Wait.waitFor(() -> { return result1.isDone(); }, 5000, 10));

        // second task should not have finished, instead wait on getting a JMS Session
        assertEquals(false, result2.isDone());

        // Only 1 session should have been created
        assertEquals(1, sessions.size());

        // The create session should have stalled waiting for a new connection
        assertFalse(Wait.waitFor(() -> { return result2.isDone(); }, 100, 10));

        cf.stop();

        // The create session should have exited on stop of the factory
        assertTrue(Wait.waitFor(() -> { return result2.isDone(); }, 5000, 10));

        // Only 1 session should have been created
        assertEquals(1, sessions.size());

        // Take all threads down
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    static class SessionTakerAndReturner implements Callable<Boolean> {

        public final static Logger TASK_LOG = LoggerFactory.getLogger(SessionTaker.class);

        /**
         * @return true if session created, false otherwise
         */
        @Override
        public Boolean call() {

            Session session = null;

            try {
                session = JmsPoolConnectionFactoryMaximumActiveTest.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                TASK_LOG.info("Created new Session with id" + session);
                JmsPoolConnectionFactoryMaximumActiveTest.addSession(session);
            } catch (Exception ex) {
                TASK_LOG.error(ex.getMessage());
                return new Boolean(false);
            } finally {
                if (session != null) {
                    try {
                        session.close();
                    } catch (JMSException e) {
                    }
                }
            }

            return new Boolean(session != null);
        }
    }

    static class SessionTaker implements Callable<Boolean> {

        public final static Logger TASK_LOG = LoggerFactory.getLogger(SessionTaker.class);

        /**
         * @return true if session created, false otherwise
         */
        @Override
        public Boolean call() {

            Session one = null;

            try {
                one = JmsPoolConnectionFactoryMaximumActiveTest.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                TASK_LOG.info("Created new Session with id" + one);
                JmsPoolConnectionFactoryMaximumActiveTest.addSession(one);
            } catch (Exception ex) {
                TASK_LOG.error(ex.getMessage());
                return new Boolean(false);
            }

            return new Boolean(one != null);
        }
    }
}
