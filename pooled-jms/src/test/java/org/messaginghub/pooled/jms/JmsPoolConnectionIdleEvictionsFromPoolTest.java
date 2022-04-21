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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import jakarta.jms.Connection;
import jakarta.jms.IllegalStateException;
import jakarta.jms.Session;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.messaginghub.pooled.jms.mock.MockJMSConnection;

@Timeout(60)
public class JmsPoolConnectionIdleEvictionsFromPoolTest extends JmsPoolTestSupport {

    @Test
    public void testEvictionOfIdle() throws Exception {
        cf.setConnectionIdleTimeout(10);
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Connection amq1 = connection.getConnection();

        connection.close();

        // let it idle timeout
        TimeUnit.MILLISECONDS.sleep(20);

        JmsPoolConnection connection2 = (JmsPoolConnection) cf.createConnection();
        Connection amq2 = connection2.getConnection();
        assertTrue(!amq1.equals(amq2), "not equal");
    }

    @Test
    public void testEvictionOfSeeminglyClosedConnection() throws Exception {
        cf.setConnectionIdleTimeout(10);
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        MockJMSConnection mockConnection1 = (MockJMSConnection) connection.getConnection();

        connection.close();

        mockConnection1.close();

        JmsPoolConnection connection2 = (JmsPoolConnection) cf.createConnection();
        Connection mockConnection2 = connection2.getConnection();

        assertNotSame(mockConnection1, mockConnection2);
    }

    @Test
    public void testNotIdledWhenInUse() throws Exception {
        cf.setConnectionIdleTimeout(10);
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // let connection to get idle
        TimeUnit.MILLISECONDS.sleep(20);

        // get a connection from pool again, it should be the same underlying connection
        // as before and should not be idled out since an open session exists.
        JmsPoolConnection connection2 = (JmsPoolConnection) cf.createConnection();
        assertSame(connection.getConnection(), connection2.getConnection());

        // now the session is closed even when it should not be
        try {
            // any operation on session first checks whether session is closed
            s.getTransacted();
        } catch (IllegalStateException e) {
            assertTrue(false, "Session should be fine, instead: " + e.getMessage());
        }

        Connection original = connection.getConnection();

        connection.close();
        connection2.close();

        // let connection to get idle
        TimeUnit.MILLISECONDS.sleep(40);

        // get a connection from pool again, it should be a new Connection instance as the
        // old one should have been inactive and idled out.
        JmsPoolConnection connection3 = (JmsPoolConnection) cf.createConnection();
        assertNotSame(original, connection3.getConnection());
    }
}
