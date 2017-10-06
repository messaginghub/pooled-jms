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
import static org.junit.Assert.assertSame;

import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.junit.Before;
import org.junit.Test;
import org.messaginghub.pooled.jms.JmsPoolConnection;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;
import org.messaginghub.pooled.jms.mock.MockJMSConnection;
import org.messaginghub.pooled.jms.mock.MockJMSConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsPoolConnectionTemporaryDestinationTest {

    private static final Logger LOG = LoggerFactory.getLogger(JmsPoolConnectionTemporaryDestinationTest.class);

    private MockJMSConnectionFactory mock;
    private JmsPoolConnectionFactory cf;

    @Before
    public void setUp() throws Exception {
        mock = new MockJMSConnectionFactory();

        // Ensure only one connection for that tests validate that shared connections
        // only destroy their own created temporary destinations.
        cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(mock);
        cf.setMaxConnections(1);
    }

    @Test(timeout = 60000)
    public void testTemporaryQueueWithMultipleConnectionUsers() throws Exception {
        JmsPoolConnection connection1 = null;
        JmsPoolConnection connection2 = null;

        MockJMSConnection pooledConnection = null;

        Session session1 = null;
        Session session2 = null;
        Queue tempQueue = null;
        Queue normalQueue = null;

        connection1 = (JmsPoolConnection) cf.createConnection();
        pooledConnection = (MockJMSConnection) connection1.getConnection();
        session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        tempQueue = session1.createTemporaryQueue();
        LOG.info("Created temporary queue named: " + tempQueue.getQueueName());

        assertEquals(1, pooledConnection.getConnectionStats().getActiveTemporaryQueueCount());

        connection2 = (JmsPoolConnection) cf.createConnection();
        assertSame(connection1.getConnection(), connection2.getConnection());
        session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        normalQueue = session2.createQueue("queue:FOO.TEST");
        LOG.info("Created queue named: " + normalQueue.getQueueName());

        // didn't create a temp queue on pooledConnection2 so we should still have a temp queue
        connection2.close();
        assertEquals(1, pooledConnection.getConnectionStats().getActiveTemporaryQueueCount());

        // after closing pooledConnection, where we created the temp queue, there should
        // be no temp queues left
        connection1.close();
        assertEquals(0, pooledConnection.getConnectionStats().getActiveTemporaryQueueCount());
    }

    @Test(timeout = 60000)
    public void testNoTemporaryQueueLeaksAfterConnectionClose() throws Exception {
        JmsPoolConnection connection = null;
        MockJMSConnection pooledConnection = null;

        Session session = null;
        Queue tempQueue = null;

        for (int i = 0; i < 10; i++) {
            connection = (JmsPoolConnection) cf.createConnection();
            pooledConnection = (MockJMSConnection) connection.getConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            tempQueue = session.createTemporaryQueue();
            LOG.info("Created queue named: " + tempQueue.getQueueName());
            connection.close();
            assertEquals(0, pooledConnection.getConnectionStats().getActiveTemporaryQueueCount());
        }

        assertEquals(10, pooledConnection.getConnectionStats().getTotalTemporaryQueuesCreated());
    }

    @Test(timeout = 60000)
    public void testNoTemporaryTopicLeaksAfterConnectionClose() throws Exception {
        JmsPoolConnection connection = null;
        MockJMSConnection pooledConnection = null;

        Session session = null;
        Topic tempTopic = null;

        for (int i = 0; i < 10; i++) {
            connection = (JmsPoolConnection) cf.createConnection();
            pooledConnection = (MockJMSConnection) connection.getConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            tempTopic = session.createTemporaryTopic();
            LOG.info("Created queue named: " + tempTopic.getTopicName());
            connection.close();
            assertEquals(0, pooledConnection.getConnectionStats().getActiveTemporaryTopicCount());
        }

        assertEquals(10, pooledConnection.getConnectionStats().getTotalTemporaryTopicsCreated());
    }
}
