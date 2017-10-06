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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;

import org.junit.Test;
import org.messaginghub.pooled.jms.JmsPoolConnection;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;
import org.messaginghub.pooled.jms.JmsPoolSession;
import org.messaginghub.pooled.jms.mock.MockJMSConnection;
import org.messaginghub.pooled.jms.mock.MockJMSConnectionFactory;
import org.messaginghub.pooled.jms.mock.MockJMSConnectionListener;
import org.messaginghub.pooled.jms.mock.MockJMSTemporaryQueue;
import org.messaginghub.pooled.jms.mock.MockJMSTemporaryTopic;
import org.messaginghub.pooled.jms.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A couple of tests against the PooledConnection class.
 */
public class JmsPoolConnectionTest extends JmsPoolTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(JmsPoolConnectionTest.class);

    @Test(timeout = 60000)
    public void testExceptionListenerGetsNotified() throws Exception {
        CountDownLatch signal = new CountDownLatch(1);
        Connection connection = cf.createConnection();
        connection.setExceptionListener(new ExceptionListener() {

            @Override
            public void onException(JMSException exception) {
                LOG.info("ExceptionListener called with error: {}", exception.getMessage());
                signal.countDown();
            }
        });

        assertNotNull(connection.getExceptionListener());

        MockJMSConnection mockJMSConnection = (MockJMSConnection) ((JmsPoolConnection) connection).getConnection();
        mockJMSConnection.injectConnectionError(new JMSException("Some non-fatal error"));

        assertTrue(signal.await(10, TimeUnit.SECONDS));
    }

    @Test(timeout = 60000)
    public void testGetConnectionMetaData() throws Exception {
        Connection connection = cf.createConnection();
        assertNotNull(connection.getMetaData());
    }

    @Test(timeout = 60000)
    public void testCreateSession() throws Exception {
        Connection connection = cf.createConnection();

        Session session1 = connection.createSession();
        Session session2 = connection.createSession();

        assertNotSame(session1, session2);
        assertEquals(session1.getAcknowledgeMode(), Session.AUTO_ACKNOWLEDGE);
        assertEquals(session2.getAcknowledgeMode(), Session.AUTO_ACKNOWLEDGE);

        JmsPoolSession wrapperSession1 = (JmsPoolSession) session1;
        JmsPoolSession wrapperSession2 = (JmsPoolSession) session2;

        assertNotSame(wrapperSession1.getSession(), wrapperSession2.getSession());
    }

    @Test(timeout = 60000)
    public void testCreateSessionAutoAcknowledge() throws Exception {
        doTestCreateSessionWithGivenAckMode(Session.AUTO_ACKNOWLEDGE);
    }

    @Test(timeout = 60000)
    public void testCreateSessionDupsOkAcknowledge() throws Exception {
        doTestCreateSessionWithGivenAckMode(Session.DUPS_OK_ACKNOWLEDGE);
    }

    @Test(timeout = 60000)
    public void testCreateSessionClientAcknowledge() throws Exception {
        doTestCreateSessionWithGivenAckMode(Session.CLIENT_ACKNOWLEDGE);
    }

    @Test(timeout = 60000)
    public void testCreateSessionSessionTransacted() throws Exception {
        doTestCreateSessionWithGivenAckMode(Session.SESSION_TRANSACTED);
    }

    public void doTestCreateSessionWithGivenAckMode(int ackMode) throws Exception {
        Connection connection = cf.createConnection();

        Session session1 = connection.createSession(ackMode);
        Session session2 = connection.createSession(ackMode);

        assertNotSame(session1, session2);
        assertEquals(session1.getAcknowledgeMode(), ackMode);
        assertEquals(session2.getAcknowledgeMode(), ackMode);

        JmsPoolSession wrapperSession1 = (JmsPoolSession) session1;
        JmsPoolSession wrapperSession2 = (JmsPoolSession) session2;

        assertNotSame(wrapperSession1.getSession(), wrapperSession2.getSession());
    }

    @Test(timeout = 60000)
    public void testCreateSessionAutoAcknowledgeNoTX() throws Exception {
        doTestCreateSessionWithGivenAckModeAndTXFlag(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Test(timeout = 60000)
    public void testCreateSessionAutoAcknowledgeTX() throws Exception {
        doTestCreateSessionWithGivenAckModeAndTXFlag(true, Session.AUTO_ACKNOWLEDGE);
    }

    @Test(timeout = 60000)
    public void testCreateSessionDupsOkAcknowledgeNoTX() throws Exception {
        doTestCreateSessionWithGivenAckModeAndTXFlag(false, Session.DUPS_OK_ACKNOWLEDGE);
    }

    @Test(timeout = 60000)
    public void testCreateSessionDupsOkAcknowledgeTX() throws Exception {
        doTestCreateSessionWithGivenAckModeAndTXFlag(true, Session.DUPS_OK_ACKNOWLEDGE);
    }

    @Test(timeout = 60000)
    public void testCreateSessionClientAcknowledgeNoTX() throws Exception {
        doTestCreateSessionWithGivenAckModeAndTXFlag(false, Session.CLIENT_ACKNOWLEDGE);
    }

    @Test(timeout = 60000)
    public void testCreateSessionClientAcknowledgeTX() throws Exception {
        doTestCreateSessionWithGivenAckModeAndTXFlag(true, Session.CLIENT_ACKNOWLEDGE);
    }

    @Test(timeout = 60000)
    public void testCreateSessionSessionTransactedNoTX() throws Exception {
        doTestCreateSessionWithGivenAckModeAndTXFlag(false, Session.SESSION_TRANSACTED);
    }

    @Test(timeout = 60000)
    public void testCreateSessionSessionTransactedTX() throws Exception {
        doTestCreateSessionWithGivenAckModeAndTXFlag(true, Session.SESSION_TRANSACTED);
    }

    public void doTestCreateSessionWithGivenAckModeAndTXFlag(boolean transacted, int ackMode) throws Exception {
        Connection connection = cf.createConnection();

        if (!transacted && ackMode == Session.SESSION_TRANSACTED) {
            try {
                connection.createSession(transacted, ackMode);
                fail("Should not allow non-transacted session with SESSION_TRANSACTED");
            } catch (JMSException jmsex) {}
        } else {
            Session session1 = connection.createSession(transacted, ackMode);
            Session session2 = connection.createSession(transacted, ackMode);

            assertNotSame(session1, session2);

            if (transacted) {
                assertEquals(session1.getAcknowledgeMode(), Session.SESSION_TRANSACTED);
                assertEquals(session2.getAcknowledgeMode(), Session.SESSION_TRANSACTED);
            } else {
                assertEquals(session1.getAcknowledgeMode(), ackMode);
                assertEquals(session2.getAcknowledgeMode(), ackMode);
            }

            JmsPoolSession wrapperSession1 = (JmsPoolSession) session1;
            JmsPoolSession wrapperSession2 = (JmsPoolSession) session2;

            assertNotSame(wrapperSession1.getSession(), wrapperSession2.getSession());
        }
    }

    @Test(timeout = 60000)
    public void testSetClientIDTwiceWithSameID() throws Exception {
        Connection connection = cf.createConnection();

        // test: call setClientID("newID") twice
        // this should be tolerated and not result in an exception
        connection.setClientID("newID");

        try {
            connection.setClientID("newID");
            connection.start();
            connection.close();
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
        Connection connection = cf.createConnection();

        // test: call setClientID() twice with different IDs
        // this should result in an IllegalStateException
        connection.setClientID("newID1");
        try {
            connection.setClientID("newID2");
            fail("calling Connection.setClientID() twice with different clientID must raise an IllegalStateException");
        } catch (IllegalStateException ise) {
            LOG.debug("Correctly received " + ise);
        } finally {
            connection.close();
            cf.stop();
        }

        LOG.debug("Test finished.");
    }

    @Test(timeout = 60000)
    public void testSetClientIDAfterConnectionStart() throws Exception {
        Connection connection = cf.createConnection();

        // test: try to call setClientID() after start()
        // should result in an exception
        try {
            connection.start();
            connection.setClientID("newID3");
            fail("Calling setClientID() after start() mut raise a JMSException.");
        } catch (IllegalStateException ise) {
            LOG.debug("Correctly received " + ise);
        } finally {
            connection.close();
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
            JmsPoolConnectionTest.LOG.error("2nd call to createSession() " +
                                           "is blocking but should have returned an error instead.");
            executor.shutdownNow();
            fail("SessionPool inside JmsPoolConnectionFactory is blocking if " +
                 "limit is exceeded but should return an exception instead.");
        }
    }

    static class TestRunner implements Callable<Boolean> {

        private static final Logger TASK_LOG = LoggerFactory.getLogger(JmsPoolConnectionTest.class);

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
                MockJMSConnectionFactory mock = new MockJMSConnectionFactory();
                cf = new JmsPoolConnectionFactory();
                cf.setConnectionFactory(mock);
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
    public void testConnectionDeletesOnlyItsOwnTempQueuesOnClose() throws JMSException {
        JmsPoolConnection connection1 = (JmsPoolConnection) cf.createConnection();
        JmsPoolConnection connection2 = (JmsPoolConnection) cf.createConnection();

        assertSame(connection1.getConnection(), connection2.getConnection());

        Set<TemporaryQueue> deleted = new HashSet<>();

        MockJMSConnection mockConnection = (MockJMSConnection) connection1.getConnection();
        mockConnection.addConnectionListener(new MockJMSConnectionListener() {

            @Override
            public void onDeleteTemporaryQueue(MockJMSTemporaryQueue queue) throws JMSException {
                deleted.add(queue);
            }
        });

        Session session1 = connection1.createSession();
        Session session2 = connection2.createSession();

        TemporaryQueue tempQueue1 = session1.createTemporaryQueue();
        TemporaryQueue tempQueue2 = session1.createTemporaryQueue();

        TemporaryQueue tempQueue3 = session2.createTemporaryQueue();
        TemporaryQueue tempQueue4 = session2.createTemporaryQueue();

        connection1.close();

        assertEquals(2, deleted.size());
        assertTrue(deleted.contains(tempQueue1));
        assertTrue(deleted.contains(tempQueue2));
        assertFalse(deleted.contains(tempQueue3));
        assertFalse(deleted.contains(tempQueue4));

        connection2.close();
        assertEquals(4, deleted.size());
        assertTrue(deleted.contains(tempQueue1));
        assertTrue(deleted.contains(tempQueue2));
        assertTrue(deleted.contains(tempQueue3));
        assertTrue(deleted.contains(tempQueue4));
    }

    @Test(timeout = 60000)
    public void testConnectionDeletesOnlyItsOwnTempTopicsOnClose() throws JMSException {
        JmsPoolConnection connection1 = (JmsPoolConnection) cf.createConnection();
        JmsPoolConnection connection2 = (JmsPoolConnection) cf.createConnection();

        assertSame(connection1.getConnection(), connection2.getConnection());

        Set<TemporaryTopic> deleted = new HashSet<>();

        MockJMSConnection mockConnection = (MockJMSConnection) connection1.getConnection();
        mockConnection.addConnectionListener(new MockJMSConnectionListener() {

            @Override
            public void onDeleteTemporaryTopic(MockJMSTemporaryTopic queue) throws JMSException {
                deleted.add(queue);
            }
        });

        Session session1 = connection1.createSession();
        Session session2 = connection2.createSession();

        TemporaryTopic tempTopic1 = session1.createTemporaryTopic();
        TemporaryTopic tempTopic2 = session1.createTemporaryTopic();

        TemporaryTopic tempTopic3 = session2.createTemporaryTopic();
        TemporaryTopic tempTopic4 = session2.createTemporaryTopic();

        connection1.close();

        assertEquals(2, deleted.size());
        assertTrue(deleted.contains(tempTopic1));
        assertTrue(deleted.contains(tempTopic2));
        assertFalse(deleted.contains(tempTopic3));
        assertFalse(deleted.contains(tempTopic4));

        connection2.close();
        assertEquals(4, deleted.size());
        assertTrue(deleted.contains(tempTopic1));
        assertTrue(deleted.contains(tempTopic2));
        assertTrue(deleted.contains(tempTopic3));
        assertTrue(deleted.contains(tempTopic4));
    }

    @Test(timeout = 60000)
    public void testConnectionIgnoresDeleteTempDestinationErrorOnClose() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();

        MockJMSConnection mockConnection = (MockJMSConnection) connection.getConnection();
        mockConnection.addConnectionListener(new MockJMSConnectionListener() {

            @Override
            public void onDeleteTemporaryQueue(MockJMSTemporaryQueue queue) throws JMSException {
                throw new IllegalStateException("Destination is in use");
            }

            @Override
            public void onDeleteTemporaryTopic(MockJMSTemporaryTopic queue) throws JMSException {
                throw new IllegalStateException("Destination is in use");
            }
        });

        Session session1 = connection.createSession();

        TemporaryQueue tempQueue = session1.createTemporaryQueue();
        assertNotNull(tempQueue);
        TemporaryTopic tempTopic = session1.createTemporaryTopic();
        assertNotNull(tempTopic);

        connection.close();
    }
}
