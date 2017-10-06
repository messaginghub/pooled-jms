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

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.messaginghub.pooled.jms.JmsPoolConnection;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;
import org.messaginghub.pooled.jms.mock.MockJMSConnection;
import org.messaginghub.pooled.jms.mock.MockJMSConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsPoolReconnectOnFailureTest extends JmsPoolTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(JmsPoolReconnectOnFailureTest.class);

    @Override
    @Before
    public void setUp() throws java.lang.Exception {
        factory = new MockJMSConnectionFactory();

        cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(factory);
        cf.setMaxConnections(1);
        cf.setReconnectOnException(true);
    }

    @Ignore("Fails for unknown reason")
    @Test(timeout = 60000)
    public void testConnectionCanBeCreatedAfterFailure() throws Exception {

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
        Queue queue = session.createQueue("test");
        MessageProducer producer = session.createProducer(queue);

        MockJMSConnection mockConnection = (MockJMSConnection) ((JmsPoolConnection) connection).getConnection();
        mockConnection.injectConnectionFailure(new IOException("Lost connection"));

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

        cf.stop();
    }
}
