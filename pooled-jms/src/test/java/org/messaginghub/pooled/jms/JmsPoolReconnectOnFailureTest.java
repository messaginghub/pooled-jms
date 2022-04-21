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

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import jakarta.jms.Connection;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.messaginghub.pooled.jms.mock.MockJMSConnection;
import org.messaginghub.pooled.jms.mock.MockJMSConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Timeout(60)
public class JmsPoolReconnectOnFailureTest extends JmsPoolTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(JmsPoolReconnectOnFailureTest.class);

    @Override
    @BeforeEach
    public void setUp(TestInfo info) throws java.lang.Exception {
        super.setUp(info);

        factory = new MockJMSConnectionFactory();

        cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(factory);
        cf.setMaxConnections(1);
    }

    @Test
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
