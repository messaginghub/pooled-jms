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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.IllegalStateException;
import javax.jms.Session;

import org.junit.Test;
import org.messaginghub.pooled.jms.mock.MockJMSConnection;

public class JmsPoolConnectionIdleEvictionsFromPoolTest extends JmsPoolTestSupport {

    @Test(timeout = 60000)
    public void testEvictionOfIdle() throws Exception {
        cf.setConnectionIdleTimeout(10);
        cf.setConnectionCheckInterval(10);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Connection amq1 = connection.getConnection();

        connection.close();

        // let it idle timeout
        TimeUnit.MILLISECONDS.sleep(20);

        JmsPoolConnection connection2 = (JmsPoolConnection) cf.createConnection();
        Connection amq2 = connection2.getConnection();
        assertTrue("not equal", !amq1.equals(amq2));
    }

    @Test(timeout = 60000)
    public void testEvictionOfSeeminglyClosedConnection() throws Exception {
        cf.setConnectionIdleTimeout(10);
        cf.setConnectionCheckInterval(50_000); // Validation check should capture and evicit this

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        MockJMSConnection mockConnection1 = (MockJMSConnection) connection.getConnection();

        connection.close();

        mockConnection1.close();

        JmsPoolConnection connection2 = (JmsPoolConnection) cf.createConnection();
        Connection mockConnection2 = connection2.getConnection();

        assertNotSame(mockConnection1, mockConnection2);
    }

    @Test(timeout = 60000)
    public void testNotIdledWhenInUse() throws Exception {
        cf.setConnectionIdleTimeout(10);
        cf.setConnectionCheckInterval(10);

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
            fail("Session should be fine, instead: " + e.getMessage());
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

    @Test
    public void testConnectionsAboveProtectedMinIdleAreNotIdledOutIfActive() throws Exception {
        cf.setConnectionIdleTimeout(15);
        cf.setConnectionCheckInterval(20);
        cf.setMaxConnections(10);

        final JmsPoolConnection safeConnection = (JmsPoolConnection) cf.createConnection();

        // get a connection from pool again, it should be another connection which is now
        // above the in-built minimum idle protections which guard the first connection and
        // would be subject to reaping on idle timeout if not active but should never be
        // reaped if active.
        final JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        assertNotSame(connection.getConnection(), safeConnection.getConnection());

        // Session we will use to check on liveness.
        final Session s = connection.createSession(Session.SESSION_TRANSACTED);

        TimeUnit.MILLISECONDS.sleep(40); // Test that connection is not idled out when borrowed

        try {
            // any operation on session first checks whether session is closed and we
            // are not expecting that here.
            s.rollback();
        } catch (IllegalStateException e) {
            fail("Session should be fine, instead: " + e.getMessage());
        }

        final Connection underlying = connection.getConnection();

        connection.close();

        TimeUnit.MILLISECONDS.sleep(40); // Test that connection is idled out when not borrowed

        try {
            // Now the connection was idle and not loaned out for long enough that
            // we expect it to have been closed.
            s.rollback();
            fail("Session should be closed because its connection was expected to closed");
        } catch (IllegalStateException e) {
        }

        // get a connection from pool again, it should be a new Connection instance as the
        // old one should have been inactive and idled out.
        final JmsPoolConnection another = (JmsPoolConnection) cf.createConnection();
        assertNotSame(underlying, another.getConnection());
    }
}
