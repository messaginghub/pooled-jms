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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.Connection;
import jakarta.jms.QueueConnectionFactory;
import jakarta.jms.TopicConnectionFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
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
@Timeout(60)
public class PooledConnectionFactoryTest extends ArtemisJmsPoolTestSupport {

    public final static Logger LOG = LoggerFactory.getLogger(PooledConnectionFactoryTest.class);

    @Test
    public void testInstanceOf() throws  Exception {
        JmsPoolConnectionFactory pcf = new JmsPoolConnectionFactory();
        assertTrue(pcf instanceof QueueConnectionFactory);
        assertTrue(pcf instanceof TopicConnectionFactory);
        pcf.stop();
    }

    @Test
    public void testClearAllConnections() throws Exception {
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

    @Test
    public void testMaxConnectionsAreCreated() throws Exception {
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

    @Test
    public void testFactoryStopStart() throws Exception {
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

    @Test
    public void testConnectionsAreRotated() throws Exception {
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

    @Test
    public void testConnectionsArePooled() throws Exception {
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
}
