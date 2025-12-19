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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.messaginghub.pooled.jms.mock.MockJMSConnection;
import org.messaginghub.pooled.jms.mock.MockJMSConnectionFactory;
import org.messaginghub.pooled.jms.mock.MockJMSConnectionMetaData;
import org.messaginghub.pooled.jms.mock.MockJMSDefaultConnectionListener;
import org.messaginghub.pooled.jms.mock.MockJMSTemporaryQueue;
import org.messaginghub.pooled.jms.mock.MockJMSTemporaryTopic;
import org.messaginghub.pooled.jms.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionMetaData;
import jakarta.jms.ExceptionListener;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TemporaryTopic;

/**
 * A couple of tests against the PooledConnection class.
 */
@Timeout(60)
public class JmsPoolConnectionTest extends JmsPoolTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(JmsPoolConnectionTest.class);

    @Test
    public void testExceptionListenerGetsNotified() throws Exception {
        final CountDownLatch signal = new CountDownLatch(1);
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

    @Test
    public void testExceptionListenerPreservedFromOriginalConnectionFactory() throws Exception {
        final CountDownLatch signal = new CountDownLatch(1);

        factory.setExceptionListener(new ExceptionListener() {

            @Override
            public void onException(JMSException exception) {
                LOG.info("ExceptionListener called with error: {}", exception.getMessage());
                signal.countDown();
            }
        });

        Connection connection = cf.createConnection();

        assertNotNull(connection.getExceptionListener());

        MockJMSConnection mockJMSConnection = (MockJMSConnection) ((JmsPoolConnection) connection).getConnection();
        mockJMSConnection.injectConnectionError(new JMSException("Some non-fatal error"));

        assertTrue(signal.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testGetConnectionMetaData() throws Exception {
        Connection connection = cf.createConnection();
        ConnectionMetaData metaData = connection.getMetaData();

        assertNotNull(metaData);
        assertSame(metaData, MockJMSConnectionMetaData.INSTANCE);
    }

    @Test
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

    @Test
    public void testCreateSessionAutoAcknowledge() throws Exception {
        doTestCreateSessionWithGivenAckMode(Session.AUTO_ACKNOWLEDGE);
    }

    @Test
    public void testCreateSessionDupsOkAcknowledge() throws Exception {
        doTestCreateSessionWithGivenAckMode(Session.DUPS_OK_ACKNOWLEDGE);
    }

    @Test
    public void testCreateSessionClientAcknowledge() throws Exception {
        doTestCreateSessionWithGivenAckMode(Session.CLIENT_ACKNOWLEDGE);
    }

    @Test
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

    @Test
    public void testCreateSessionAutoAcknowledgeNoTX() throws Exception {
        doTestCreateSessionWithGivenAckModeAndTXFlag(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Test
    public void testCreateSessionAutoAcknowledgeTX() throws Exception {
        doTestCreateSessionWithGivenAckModeAndTXFlag(true, Session.AUTO_ACKNOWLEDGE);
    }

    @Test
    public void testCreateSessionDupsOkAcknowledgeNoTX() throws Exception {
        doTestCreateSessionWithGivenAckModeAndTXFlag(false, Session.DUPS_OK_ACKNOWLEDGE);
    }

    @Test
    public void testCreateSessionDupsOkAcknowledgeTX() throws Exception {
        doTestCreateSessionWithGivenAckModeAndTXFlag(true, Session.DUPS_OK_ACKNOWLEDGE);
    }

    @Test
    public void testCreateSessionClientAcknowledgeNoTX() throws Exception {
        doTestCreateSessionWithGivenAckModeAndTXFlag(false, Session.CLIENT_ACKNOWLEDGE);
    }

    @Test
    public void testCreateSessionClientAcknowledgeTX() throws Exception {
        doTestCreateSessionWithGivenAckModeAndTXFlag(true, Session.CLIENT_ACKNOWLEDGE);
    }

    @Test
    public void testCreateSessionSessionTransactedNoTX() throws Exception {
        doTestCreateSessionWithGivenAckModeAndTXFlag(false, Session.SESSION_TRANSACTED);
    }

    @Test
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

    @Test
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

    @Test
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

    @Test
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
                cf.setMaxSessionsPerConnection(1);
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
                         "is exhausted. This did not happen and indicates a problem");
                    return Boolean.FALSE;
                } catch (JMSException ex) {
                    if (ex.getCause().getClass() == java.util.NoSuchElementException.class) {
                        // expected, ignore but log
                        TASK_LOG.info("Caught expected " + ex);
                    } else {
                        TASK_LOG.error("Error trapped", ex);
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
                TASK_LOG.error(ex.getMessage());
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
    public void testConnectionDeletesOnlyItsOwnTempQueuesOnClose() throws JMSException {
        JmsPoolConnection connection1 = (JmsPoolConnection) cf.createConnection();
        JmsPoolConnection connection2 = (JmsPoolConnection) cf.createConnection();

        assertSame(connection1.getConnection(), connection2.getConnection());

        final Set<TemporaryQueue> deleted = new HashSet<>();

        MockJMSConnection mockConnection = (MockJMSConnection) connection1.getConnection();
        mockConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

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

    @Test
    public void testConnectionDeletesOnlyItsOwnTempTopicsOnClose() throws JMSException {
        JmsPoolConnection connection1 = (JmsPoolConnection) cf.createConnection();
        JmsPoolConnection connection2 = (JmsPoolConnection) cf.createConnection();

        assertSame(connection1.getConnection(), connection2.getConnection());

        final Set<TemporaryTopic> deleted = new HashSet<>();

        MockJMSConnection mockConnection = (MockJMSConnection) connection1.getConnection();
        mockConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

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

    @Test
    public void testConnectionIgnoresDeleteTempDestinationErrorOnClose() throws JMSException {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();

        MockJMSConnection mockConnection = (MockJMSConnection) connection.getConnection();
        mockConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

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

    @Test
    public void testConnectionFailures() throws Exception {
        final CountDownLatch failed = new CountDownLatch(1);

        Connection connection = cf.createConnection();
        LOG.info("Fetched new connection from the pool: {}", connection);
        connection.setExceptionListener(new ExceptionListener() {

            @Override
            public void onException(JMSException exception) {
                LOG.info("Pooled Connection failed");
                failed.countDown();
            }
        });

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getTestName());
        MessageProducer producer = session.createProducer(queue);

        MockJMSConnection mockJMSConnection = (MockJMSConnection) ((JmsPoolConnection) connection).getConnection();
        mockJMSConnection.injectConnectionError(new JMSException("Some non-fatal error"));

        assertTrue(failed.await(15, TimeUnit.SECONDS));

        try {
            producer.send(session.createMessage());
            fail("Should be disconnected");
        } catch (JMSException ex) {
            LOG.info("Producer failed as expected: {}", ex.getMessage());
        }

        Connection connection2 = cf.createConnection();
        assertNotSame(connection, connection2);
        LOG.info("Fetched new connection from the pool: {}", connection2);
        session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);

        connection2.close();
    }

    @Test
    public void testConnectionFailuresWhenMarkedFaultTolerant() throws Exception {
        final CountDownLatch failed = new CountDownLatch(1);

        cf.setFaultTolerantConnections(true); // Should keep connection open on exception.

        final JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();

        LOG.info("Fetched new connection from the pool: {}", connection);
        connection.setExceptionListener(new ExceptionListener() {

            @Override
            public void onException(JMSException exception) {
                LOG.info("Pooled Connection failed");
                failed.countDown();
            }
        });

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getTestName());
        MessageProducer producer = session.createProducer(queue);

        MockJMSConnection mockJMSConnection = (MockJMSConnection) connection.getConnection();
        mockJMSConnection.injectConnectionError(new JMSException("Some non-fatal error"));

        assertTrue(failed.await(15, TimeUnit.SECONDS));

        try {
            producer.send(session.createMessage());
            LOG.info("Producer sent message after onException as expected: {}");
        } catch (JMSException ex) {
            fail("Should not be disconnected since fault tolerant is assumed");
        }

        final JmsPoolConnection connection2 = (JmsPoolConnection) cf.createConnection();

        assertNotSame(connection, connection2);
        assertSame(connection.getConnection(), connection2.getConnection());
        LOG.info("Fetched new connection from the pool: {}", connection2);
        session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);

        connection2.close();
    }
}
