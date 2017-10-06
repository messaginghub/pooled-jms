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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jms.Connection;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Session;

import org.junit.Before;
import org.junit.Test;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;
import org.messaginghub.pooled.jms.mock.MockJMSConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsPoolSessionExhaustionTest extends JmsPoolTestSupport {

    public final static Logger LOG = LoggerFactory.getLogger(JmsPoolSessionExhaustionTest.class);

    @Override
	@Before
    public void setUp() throws Exception {
        factory = new MockJMSConnectionFactory();
        cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(factory);
        cf.setMaxConnections(1);
        cf.setBlockIfSessionPoolIsFull(false);
        cf.setMaximumActiveSessionPerConnection(1);
    }

    @Test(timeout = 60000)
    public void testCreateSessionThrowsWhenSessionPoolExhaustedSharedConnectionNoTimeout() throws JMSException {
        doTestCreateSessionThrowsWhenSessionPoolExhaustedSharedConnection(false);
    }

    @Test(timeout = 60000)
    public void testCreateSessionThrowsWhenSessionPoolExhaustedSharedConnectionTimeout() throws JMSException {
        doTestCreateSessionThrowsWhenSessionPoolExhaustedSharedConnection(true);
    }

    private void doTestCreateSessionThrowsWhenSessionPoolExhaustedSharedConnection(boolean timeout) throws JMSException {
        if (timeout) {
            cf.setBlockIfSessionPoolIsFull(true);
            cf.setBlockIfSessionPoolIsFullTimeout(50);
        } else {
            cf.setBlockIfSessionPoolIsFull(false);
        }

        Connection connection1 = cf.createConnection();
        Connection connection2 = cf.createConnection();

        // One Connections should be able to create one session
        Session session1 = connection1.createSession();

        long startTime = System.currentTimeMillis();
        try {
            connection2.createSession();
            fail("Should not be able to create a second Session Connection 2");
        } catch (IllegalStateException ex) {
            if (timeout) {
                assertTrue((System.currentTimeMillis() - startTime) > 30);
            }
            LOG.info("Caught exception on session create as expected: {}", ex.getMessage());
        }

        assertNotNull(session1);

        startTime = System.currentTimeMillis();
        try {
            connection1.createSession();
            fail("Should not be able to create a second Session on Connection 1");
        } catch (IllegalStateException ex) {
            if (timeout) {
                assertTrue((System.currentTimeMillis() - startTime) > 30);
            }
            LOG.info("Caught exception on session create as expected: {}", ex.getMessage());
        }

        session1.close();

        // Now the other Connections should be able to create one session
        Session session2 = connection2.createSession();

        assertNotNull(session1);
        assertNotNull(session2);
        assertNotSame(session1, session2);
    }

    @Test(timeout = 60000)
    public void testCreateSessionThrowsWhenSessionPoolExhaustedNonSharedConnectionNoTimeout() throws JMSException {
        doTestCreateSessionThrowsWhenSessionPoolExhaustedNonSharedConnection(false);
    }

    @Test(timeout = 60000)
    public void testCreateSessionThrowsWhenSessionPoolExhaustedNonSharedConnectionWithTimeout() throws JMSException {
        doTestCreateSessionThrowsWhenSessionPoolExhaustedNonSharedConnection(true);
    }

    private void doTestCreateSessionThrowsWhenSessionPoolExhaustedNonSharedConnection(boolean timeout) throws JMSException {

        cf.setMaxConnections(2);
        if (timeout) {
            cf.setBlockIfSessionPoolIsFull(true);
            cf.setBlockIfSessionPoolIsFullTimeout(50);
        } else {
            cf.setBlockIfSessionPoolIsFull(false);
        }

        Connection connection1 = cf.createConnection();
        Connection connection2 = cf.createConnection();

        // Both Connections should be able to create one session
        Session session1 = connection1.createSession();
        Session session2 = connection2.createSession();

        assertNotNull(session1);
        assertNotNull(session2);
        assertNotSame(session1, session2);

        long startTime = System.currentTimeMillis();
        try {
            connection1.createSession();
            fail("Should not be able to create a second Session on Connection 1");
        } catch (IllegalStateException ex) {
            if (timeout) {
                assertTrue((System.currentTimeMillis() - startTime) > 30);
            }
            LOG.info("Caught exception on session create as expected: {}", ex.getMessage());
        }

        startTime = System.currentTimeMillis();
        try {
            connection2.createSession();
            fail("Should not be able to create a second Session Connection 2");
        } catch (IllegalStateException ex) {
            if (timeout) {
                assertTrue((System.currentTimeMillis() - startTime) > 30);
            }
            LOG.info("Caught exception on session create as expected: {}", ex.getMessage());
        }

        session1.close();
        session2.close();

        // Both Connections should be able to create one session
        Session session3 = connection1.createSession();
        Session session4 = connection2.createSession();

        assertNotNull(session3);
        assertNotNull(session4);
        assertNotSame(session1, session3);
        assertNotSame(session2, session4);
        assertNotSame(session3, session4);
    }
}
